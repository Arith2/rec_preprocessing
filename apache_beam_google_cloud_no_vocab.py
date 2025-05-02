# Copyright 2024 The TensorFlow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""TFX beam preprocessing pipeline for Criteo data.

Preprocessing util for criteo data. Transformations:
1. Fill missing features with zeros.
2. Set negative integer features to zeros.
3. Normalize integer features using log(x+1).
4. For categorical features (hex), convert to integer and take value modulus the
   max_vocab_size value.

Usage:
For raw Criteo data, this script should be run twice.
First run should set vocab_gen_mode to true.  This run is used to generate
  vocabulary files in the temp_dir location.
Second run should set vocab_gen_mode to false.  It is necessary to point to the
  same temp_dir used during the first run.
"""

import argparse
import datetime
import os
from absl import logging

import apache_beam as beam
import numpy as np
import tensorflow as tf, tf_keras
import tensorflow_transform as tft
import tensorflow_transform.beam as tft_beam
from tensorflow_transform.tf_metadata import dataset_metadata
from tensorflow_transform.tf_metadata import schema_utils
from tfx_bsl.public import tfxio


parser = argparse.ArgumentParser()
parser.add_argument(
    "--input_path",
    default=None,
    required=True,
    help="Input path. Be sure to set this to cover all data, to ensure "
    "that sparse vocabs are complete.")
parser.add_argument(
    "--output_path",
    default=None,
    required=True,
    help="Output path.")
parser.add_argument(
    "--temp_dir",
    default=None,
    required=True,
    help="Directory to store temporary metadata. Important because vocab "
         "dictionaries will be stored here. Co-located with data, ideally.")
parser.add_argument(
    "--csv_delimeter",
    default="\t",
    help="Delimeter string for input and output.")
parser.add_argument(
    "--vocab_gen_mode",
    action="store_true",
    default=False,
    help="If it is set, process full dataset and do not write CSV output. In "
         "this mode, See temp_dir for vocab files. input_path should cover all "
         "data, e.g. train, test, eval.")
parser.add_argument(
    "--runner",
    help="Runner for Apache Beam, needs to be one of {DirectRunner, "
    "DataflowRunner}.",
    default="DirectRunner")
parser.add_argument(
    "--project",
    default=None,
    help="ID of your project. Ignored by DirectRunner.")
parser.add_argument(
    "--region",
    default=None,
    help="Region. Ignored by DirectRunner.")
parser.add_argument(
    "--max_vocab_size",
    type=int,
    default=10_000_000,
    help="Max index range, categorical features convert to integer and take "
         "value modulus the max_vocab_size")
parser.add_argument(
    "--machine_type",
    default="n2-standard-16",
    help="Machine type for workers")
parser.add_argument(
    "--num_workers",
    type=int,
    default=1,
    help="Number of workers")
parser.add_argument(
    "--sdk_container_image",
    default="gcr.io/cloud-shared-execution/beam-custom:latest",
    help="Container image for the Beam job")
parser.add_argument(
    "--max_num_workers",
    type=int,
    default=1,
    help="Maximum number of workers")
parser.add_argument(
    "--autoscalingAlgorithm",
    default="THROUGHPUT_BASED",
    help="Autoscaling algorithm")

args = parser.parse_args()

NUM_NUMERIC_FEATURES = 13

NUMERIC_FEATURE_KEYS = [
    f"col_{x+1}" for x in range(NUM_NUMERIC_FEATURES)]
CATEGORICAL_FEATURE_KEYS = [
    f"col_{x}" for x in range(NUM_NUMERIC_FEATURES + 1, 40)]
LABEL_KEY = "col_0"


# Data is first preprocessed in pure Apache Beam using numpy.
# This removes missing values and hexadecimal-encoded values.
# For the TF schema, we can thus specify the schema as FixedLenFeature
# for TensorFlow Transform.
FEATURE_SPEC = dict([(name, tf.io.FixedLenFeature([], dtype=tf.string))
                     for name in CATEGORICAL_FEATURE_KEYS] +
                    [(name, tf.io.FixedLenFeature([], dtype=tf.float32))
                     for name in NUMERIC_FEATURE_KEYS] +
                    [(LABEL_KEY, tf.io.FixedLenFeature([], tf.int64))])
INPUT_METADATA = dataset_metadata.DatasetMetadata(
    schema_utils.schema_from_feature_spec(FEATURE_SPEC))


def apply_vocab_fn(inputs):
  """Preprocessing fn for sparse features.

  Applies vocab to bucketize sparse features. This function operates using
  previously-created vocab files.
  Pre-condition: Full vocab has been materialized.

  Args:
    inputs: Input features to transform.

  Returns:
    Output dict with transformed features.
  """
  outputs = {}

  outputs[LABEL_KEY] = inputs[LABEL_KEY]
  for key in NUMERIC_FEATURE_KEYS:
    outputs[key] = inputs[key]
  for idx, key in enumerate(CATEGORICAL_FEATURE_KEYS):
    vocab_fn = os.path.join(
        args.temp_dir, "tftransform_tmp", "feature_{}_vocab".format(idx))
    outputs[key] = tft.apply_vocabulary(inputs[key], vocab_fn)

  return outputs


def compute_vocab_fn(inputs):
  """Preprocessing fn for sparse features.

  This function computes unique IDs for the sparse features. We rely on implicit
  behavior which writes the vocab files to the vocab_filename specified in
  tft.compute_and_apply_vocabulary.

  Pre-condition: Sparse features have been converted to integer and mod'ed with
  args.max_vocab_size.

  Args:
    inputs: Input features to transform.

  Returns:
    Output dict with transformed features.
  """
  outputs = {}

  outputs[LABEL_KEY] = inputs[LABEL_KEY]
  for key in NUMERIC_FEATURE_KEYS:
    outputs[key] = inputs[key]
  for idx, key in enumerate(CATEGORICAL_FEATURE_KEYS):
    outputs[key] = tft.compute_and_apply_vocabulary(
        x=inputs[key],
        vocab_filename="feature_{}_vocab".format(idx))

  return outputs

# class FillMissing(beam.DoFn):
#   """Fills missing elements in a dictionary with zero-equivalent values."""

#   def process(self, element):
#     output = element.as_dict() if hasattr(element, 'as_dict') else dict(element)
#     for key, value in element.items():
#       if value is None or value == "":
#         if key.startswith("col_") and int(key.split("_")[1]) <= 13:
#           output[key] = 0.0  # float for dense
#         else:
#           output[key] = "0"  # string for sparse
#       else:
#         output[key] = value
#     yield output



class NegsToZeroLog(beam.DoFn):
  """Sets negative dense values to zero, then log(x+1)."""

  def process(self, element):
    # Create a new dictionary with all fields
    output = {}
    
    # First, copy all fields from the BeamSchema_ object
    for key in dir(element):
      # Skip special methods and attributes (those starting with '_')
      if not key.startswith('_') and key != 'as_dict' and key != 'keys' and key != 'values':
        output[key] = getattr(element, key)
    
    # Process numeric features
    for i in range(1, NUM_NUMERIC_FEATURES + 1):
      key = f"col_{i}"
      val = getattr(element, key, 0.0)
      try:
        val = float(val)
        val = 0.0 if val < 0 else np.log(val + 1)
      except Exception:
        val = 0.0
      output[key] = val
      
    yield output



class HexToIntModRange(beam.DoFn):
  """Converts hex string to integer and applies modulo."""

  def process(self, element):
    # Create a new dictionary with all fields
    output = {}
    
    # First, copy all fields from the BeamSchema_ object
    for key in dir(element):
      # Skip special methods and attributes
      if not key.startswith('_') and key != 'as_dict' and key != 'keys' and key != 'values':
        output[key] = getattr(element, key)
    
    # Process categorical features
    for i in range(NUM_NUMERIC_FEATURES + 1, 40):
      key = f"col_{i}"
      val = getattr(element, key, "0")
      try:
        val = int(val, 16) % args.max_vocab_size
      except Exception:
        val = 0
      output[key] = val
      
    yield output


def transform_data(data_path, output_path):
  """Preprocesses Criteo data.

  Two processing modes are supported. Raw data will require two passes.
  If full vocab files already exist, only one pass is necessary.

  Args:
    data_path: File(s) to read.
    output_path: Path to which output CSVs are written, if necessary.
  """

  preprocessing_fn = compute_vocab_fn if args.vocab_gen_mode else apply_vocab_fn

  gcp_project = args.project
  region = args.region

  job_name = (f"criteo-preprocessing-"
              f"{datetime.datetime.now().strftime('%y%m%d-%H%M%S')}")

  # set up Beam pipeline.
  pipeline_options = None

  if args.runner == "DataflowRunner":
    options = {
        "runner": "DataflowRunner",
        "staging_location": os.path.join(output_path, "tmp", "staging"),
        "temp_location": os.path.join(output_path, "tmp"),
        "job_name": job_name,
        "project": gcp_project,
        "save_main_session": True,
        "region": region,
        # "setup_file": "./setup.py",
        "machine_type": args.machine_type,  # Add this line
        "num_workers": args.num_workers,  # Add this line
        "sdk_container_image": args.sdk_container_image,
        "autoscalingAlgorithm": args.autoscalingAlgorithm,
        "max_num_workers": args.max_num_workers,
    }
    pipeline_options = beam.pipeline.PipelineOptions(flags=[], **options)
  elif args.runner == "DirectRunner":
    pipeline_options = beam.options.pipeline_options.DirectOptions(
        direct_num_workers=os.cpu_count(),
        direct_running_mode="multi_threading")
    
  print(f"RUNNER: {pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).runner}")
#   standard_options = pipeline_options.view_as(beam.options.pipeline_options.StandardOptions)
#   standard_options.enforce_runner_api = True
#   standard_options.enforce_runner_required = True  # Only in Beam 2.51+



#   with beam.Pipeline(args.runner, options=pipeline_options) as pipeline:
  with beam.Pipeline(options=pipeline_options) as pipeline:
    with tft_beam.Context(temp_dir=args.temp_dir):
      
      ordered_columns = [f"col_{i}" for i in range(40)]

      processed_lines = (
          pipeline
          | 'Read Parquet File' >> beam.io.ReadFromParquet(file_pattern=data_path, as_rows=False)
          | 'Take First 10 Rows' >> beam.combiners.Sample.FixedSizeGlobally(10)
          # For numerical features, set negatives to zero. Then take log(x+1).
          | "NegsToZeroLog" >> beam.ParDo(NegsToZeroLog())
          # For categorical features, mod the values with vocab size.
          | "HexToIntModRange" >> beam.ParDo(HexToIntModRange())
        # pipeline
        # | 'Read Parquet File' >> beam.io.ReadFromParquet(file_pattern=data_path, as_rows=True)
        # | 'Take First 10 Rows' >> beam.combiners.Sample.FixedSizeGlobally(10)
        # | 'Print Sample' >> beam.FlatMap(lambda rows: print("\n".join([str(row) for row in rows])))
      )


    #   # CSV reader: List the cols in order, as dataset schema is not ordered.
    #   ordered_columns = [LABEL_KEY
    #                     ] + NUMERIC_FEATURE_KEYS + CATEGORICAL_FEATURE_KEYS

    #   csv_tfxio = tfxio.BeamRecordCsvTFXIO(
    #       physical_format="text",
    #       column_names=ordered_columns,
    #       delimiter=args.csv_delimeter,
    #       schema=INPUT_METADATA.schema)

    #   converted_data = (
    #       processed_lines
    #       | "DecodeData" >> csv_tfxio.BeamSource())

    #   raw_dataset = (converted_data, csv_tfxio.TensorAdapterConfig())

    #   # The TFXIO output format is chosen for improved performance.
    #   transformed_dataset, _ = (
    #       raw_dataset | tft_beam.AnalyzeAndTransformDataset(
    #           preprocessing_fn, output_record_batches=False))

    #   # Transformed metadata is not necessary for encoding.
    #   transformed_data, transformed_metadata = transformed_dataset

    #   if not args.vocab_gen_mode:
    #     # Write to CSV.
    #     transformed_csv_coder = tft.coders.CsvCoder(
    #         ordered_columns, transformed_metadata.schema,
    #         delimiter=args.csv_delimeter)
    #     _ = (
    #         transformed_data
    #         | "EncodeDataCsv" >> beam.Map(transformed_csv_coder.encode)
    #         | "WriteDataCsv" >> beam.io.WriteToText(output_path))


if __name__ == "__main__":
  logging.set_verbosity(logging.INFO)

  transform_data(data_path=args.input_path,
                 output_path=args.output_path)