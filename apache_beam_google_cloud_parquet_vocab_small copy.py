"""
Minimal working Apache Beam + TFT pipeline for Parquet input
Using `as_rows=False` to emit dictionaries instead of Beam Rows.
"""

import apache_beam as beam
import apache_beam.transforms.util as beam_util
import numpy as np
import tensorflow_transform as tft
import tensorflow_transform.beam as tft_beam
from tensorflow_transform.tf_metadata import dataset_metadata, schema_utils
import tensorflow as tf
import argparse
import os
import datetime
from absl import logging
import time
import json
from apache_beam.ml.transforms.tft import ComputeAndApplyVocabulary


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
    default="europe-west1-docker.pkg.dev/cloud-shared-execution/beam-docker/beam-tft115:v1",
    help="Container image for the Beam job")
parser.add_argument(
    "--max_num_workers",
    type=int,
    default=1,
    help="Maximum number of workers")
parser.add_argument(
    "--autoscaling_algorithm",
    default="THROUGHPUT_BASED",
    help="Autoscaling algorithm")
parser.add_argument(
    "--job_name",
    default="criteo-preprocess",
    help="Job name")
parser.add_argument(
    "--csv_delimeter",
    default="\t",
    help="Delimeter string for input and output.")

args = parser.parse_args()

NUM_NUMERIC_FEATURES = 13
NUMERIC_KEYS = [f"col_{i+1}" for i in range(NUM_NUMERIC_FEATURES)]
CATEGORICAL_KEYS = [f"col_{i}" for i in range(NUM_NUMERIC_FEATURES + 1, 40)]
LABEL_KEY = "col_0"

# FEATURE_SPEC = {
#     LABEL_KEY: tf.io.FixedLenFeature([], tf.int64),
#     **{k: tf.io.FixedLenFeature([], tf.float32) for k in NUMERIC_KEYS},
#     **{k: tf.io.FixedLenFeature([], tf.string) for k in CATEGORICAL_KEYS},
# }
FEATURE_SPEC = dict([(LABEL_KEY, tf.io.FixedLenFeature([], tf.int64))]
                    + [(name, tf.io.FixedLenFeature([], dtype=tf.float32))
                     for name in NUMERIC_KEYS]
                     + [(name, tf.io.FixedLenFeature([], dtype=tf.int64))
                     for name in CATEGORICAL_KEYS])
INPUT_METADATA = dataset_metadata.DatasetMetadata(
    schema_utils.schema_from_feature_spec(FEATURE_SPEC))

def compute_vocab_fn(inputs):
    outputs = {k: inputs[k] for k in [LABEL_KEY] + NUMERIC_KEYS}
    for idx, key in enumerate(CATEGORICAL_KEYS):
        outputs[key] = tft.compute_and_apply_vocabulary(
            x=inputs[key],
            vocab_filename=f"feature_{idx}_vocab"
        )
    return outputs

def apply_vocab_fn(inputs):
    outputs = {k: inputs[k] for k in [LABEL_KEY] + NUMERIC_KEYS}
    for idx, key in enumerate(CATEGORICAL_KEYS):
        vocab_path = os.path.join(args.temp_dir, 'tftransform_tmp', f'feature_{idx}_vocab')
        outputs[key] = tft.apply_vocabulary(inputs[key], vocab_path)
    return outputs

class PreprocessDict(beam.DoFn):
    def __init__(self):
        self.skipped_counter = beam.metrics.Metrics.counter("preprocessing", "skipped_elements")

    def process(self, element):
        try: 
            result = dict(element)
            for key in NUMERIC_KEYS:
                try:
                    val = float(result.get(key, 0.0))
                    result[key] = float(np.log(val + 1) if val >= 0 else 0.0)
                except:
                    result[key] = 0.0
            for key in CATEGORICAL_KEYS:
                val = result.get(key, "0")
                try:
                    result[key] = int(val) % args.max_vocab_size
                except:
                    result[key] = 0
            return [result]
        except Exception as e:
            logging.error(f"Error processing element: {element}, error: {e}")
            return []

def measure_time(description):
    """Create a DoFn that logs timestamps for measuring performance"""
    class MeasureTimeFn(beam.DoFn):
        def __init__(self, description):
            self.description = description
            
        def setup(self):
            self.count = 0
            self.start_time = time.time()
            logging.info(f"Starting {self.description}")
            
        def process(self, element):
            self.count += 1
            if self.count % 1000 == 0:
                elapsed = time.time() - self.start_time
                logging.info(f"{self.description}: Processed {self.count} elements in {elapsed:.2f}s ({self.count/elapsed:.2f} elements/s)")
            yield element
            
        def finish_bundle(self):
            elapsed = time.time() - self.start_time
            logging.info(f"Finished {self.description}: Processed {self.count} elements in {elapsed:.2f}s ({self.count/elapsed:.2f} elements/s)")
            
    return MeasureTimeFn(description)

def transform_data():
    # job_name = f"criteo-preprocess-{datetime.datetime.now().strftime('%y%m%d-%H%M%S')}"
    options_dict = {
        "runner": "DataflowRunner",
        "staging_location": os.path.join(args.output_path, "tmp", "staging"),
        "temp_location": os.path.join(args.output_path, "tmp"),
        "job_name": args.job_name,
        "project": args.project,
        "save_main_session": True,
        "region": args.region,
        # "setup_file": "./setup.py",
        "machine_type": args.machine_type,  # Add this line
        "num_workers": args.num_workers,  # Add this line
        "sdk_container_image": args.sdk_container_image,
        # "wait_until_finish": True,
        "autoscaling_algorithm": args.autoscaling_algorithm,
        "max_num_workers": args.max_num_workers, 
    } if args.runner == "DataflowRunner" else {}

    # Add these debug lines:

    logging.info(f"Pipeline options_dict: {json.dumps(options_dict, indent=2)}")
    for key, value in options_dict.items():
        logging.info(f"  {key}: {value}")

    pipeline_options = beam.pipeline.PipelineOptions(flags=[], **options_dict)

    # pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).sdk_container_image = args.sdk_container_image
    # pipeline_options.view_as(beam.options.pipeline_options.DebugOptions).experiments = ['use_runner_v2']

    logging.info(f"Starting Parquet read test from: {args.input_path}")
    logging.info(f"Runner: {args.runner}")
    logging.info(f"Number of workers: {args.num_workers}")

    with beam.Pipeline(options=pipeline_options) as p:
        with tft_beam.Context(temp_dir=args.temp_dir):
            raw_data = (
                p
                | 'ReadParquet' >> beam.io.ReadFromParquet(args.input_path, validate=False, as_rows=False)
                | 'MeasureRead' >> beam.ParDo(measure_time("ReadParquet output"))
                | 'PreprocessDict' >> beam.ParDo(PreprocessDict())  
            )

            

            # # Apply vocab to categorical features
            # for key in CATEGORICAL_KEYS:
            #     raw_data = raw_data | f'ComputeVocab_{key}' >> ComputeAndApplyVocabulary(lambda x, key=key: x[key],
            #                                                                      vocab_size=args.max_vocab_size,
            #                                                                      output_key=key)

            # raw_dataset = (raw_data, INPUT_METADATA)
            
            # transform_fn = compute_vocab_fn if args.vocab_gen_mode else apply_vocab_fn

            # transformed_dataset, _ = (
            #     raw_dataset
            #     | 'Transform' >> tft_beam.AnalyzeAndTransformDataset(transform_fn)
            # )

            # transformed_data, _ = transformed_dataset
            # _ = (
            #     transformed_data
            #     | 'WriteOut' >> beam.io.WriteToText(args.output_path)
            # )

if __name__ == '__main__':
    logging.set_verbosity(logging.INFO)
    start_time = time.time()
    transform_data()
    total_time = time.time() - start_time
    logging.info(f"Total pipeline execution time: {total_time:.2f} seconds")
