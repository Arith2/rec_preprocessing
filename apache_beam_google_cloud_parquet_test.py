"""
Test script to measure Parquet reading performance from Google Cloud Storage
"""

import apache_beam as beam
import argparse
import os
import time
from absl import logging
import tensorflow as tf
import numpy as np
from tensorflow_transform.tf_metadata import dataset_metadata, schema_utils

parser = argparse.ArgumentParser()
parser.add_argument(
    "--input_path",
    required=True,
    help="Input path to Parquet files in GCS")
parser.add_argument(
    "--output_path",
    required=True,
    help="Output path for results")
parser.add_argument(
    "--temp_dir",
    required=True,
    help="Temporary directory")
parser.add_argument(
    "--runner",
    default="DirectRunner",
    help="Runner for Apache Beam")
parser.add_argument(
    "--project",
    default=None,
    help="GCP project ID")
parser.add_argument(
    "--region",
    default=None,
    help="GCP region")
parser.add_argument(
    "--machine_type",
    default="n2-standard-16",
    help="Machine type for workers")
parser.add_argument(
    "--num_workers",
    type=int,
    default=8,
    help="Number of workers")
parser.add_argument(
    "--sdk_container_image",
    default="gcr.io/cloud-shared-execution/beam-custom:latest",
    help="Container image")
parser.add_argument(
    "--job_name",
    default="parquet-read-test",
    help="Job name")

args = parser.parse_args()

NUM_NUMERIC_FEATURES = 13
NUMERIC_KEYS = [f"col_{i+1}" for i in range(NUM_NUMERIC_FEATURES)]
CATEGORICAL_KEYS = [f"col_{i}" for i in range(NUM_NUMERIC_FEATURES + 1, 40)]
LABEL_KEY = "col_0"

FEATURE_SPEC = {
    LABEL_KEY: tf.io.FixedLenFeature([], tf.int64),
    **{k: tf.io.FixedLenFeature([], tf.float32) for k in NUMERIC_KEYS},
    **{k: tf.io.FixedLenFeature([], tf.string) for k in CATEGORICAL_KEYS},
}
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
    def process(self, element):
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
                result[key] = str(int(val, 16) % args.max_vocab_size)
            except:
                result[key] = "0"
        return [result]

def print_element(element):
    # Just convert to string and return - lightweight operation
    return str(element)[:100] + "..."  # Truncate to avoid large logs

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
    """Simple pipeline to test Parquet reading performance"""
    options_dict = {
        "runner": args.runner,
        "project": args.project,
        "region": args.region,
        "staging_location": os.path.join(args.output_path, "staging"),
        "temp_location": args.temp_dir,
        "job_name": args.job_name,
        "save_main_session": True,
        "machine_type": args.machine_type,
        "num_workers": args.num_workers,
        "sdk_container_image": args.sdk_container_image
    } if args.runner == "DataflowRunner" else {}

    pipeline_options = beam.pipeline.PipelineOptions(flags=[], **options_dict)
    
    logging.info(f"Starting Parquet read test from: {args.input_path}")
    logging.info(f"Runner: {args.runner}")
    logging.info(f"Number of workers: {args.num_workers}")

    with beam.Pipeline(options=pipeline_options) as p:
        
        # Read Parquet and count records
        read_result = (
            p
            | 'ReadParquet' >> beam.io.ReadFromParquet(
                args.input_path, 
                validate=False,
                # columns=['col_0', 'col_1', 'col_2']
                )
            | 'MeasureRead' >> beam.ParDo(measure_time("Parquet Reading"))
            | 'Count' >> beam.combiners.Count.Globally()
            | 'LogCount' >> beam.Map(lambda count: logging.info(f"Total records read: {count}"))
        )

if __name__ == '__main__':
    logging.set_verbosity(logging.INFO)
    start_time = time.time()
    transform_data()
    total_time = time.time() - start_time
    logging.info(f"Total pipeline execution time: {total_time:.2f} seconds")
