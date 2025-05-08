import warnings
warnings.filterwarnings("ignore")
warnings.simplefilter("ignore", UserWarning)

import os
import sys
import argparse
import glob
import time
import numpy as np
import shutil
import numba
from urllib.parse import urlparse
from datetime import datetime
from tqdm import tqdm

import dask_cudf
from dask_cuda import LocalCUDACluster
from dask.distributed import Client, progress
import gcsfs

import nvtabular as nvt
from merlin.core.compat import device_mem_size, pynvml_mem_size
from nvtabular.ops import (
    Categorify,
    Clip,
    FillMissing,
    Normalize,
    get_embedding_sizes,
    LambdaOp,
    LogOp,
)

import logging
import cudf
import pandas as pd

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'preprocessing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)

# Log versions for debugging
logging.info(f"NVTabular version: {nvt.__version__}")
logging.info(f"cuDF version: {cudf.__version__}")
logging.info(f"Pandas version: {pd.__version__}")

# Define dataset schema
CATEGORICAL_COLUMNS = ["col_" + str(x) for x in range(14, 40)]
CONTINUOUS_COLUMNS = ["col_" + str(x) for x in range(1, 14)]
LABEL_COLUMNS = ['col_0']
COLUMNS = LABEL_COLUMNS + CONTINUOUS_COLUMNS + CATEGORICAL_COLUMNS
# /samples/criteo mode doesn't have dense features
CRITEO_COLUMN = LABEL_COLUMNS + CATEGORICAL_COLUMNS

NUM_INTEGER_COLUMNS = 13
NUM_CATEGORICAL_COLUMNS = 26
NUM_TOTAL_COLUMNS = 1 + NUM_INTEGER_COLUMNS + NUM_CATEGORICAL_COLUMNS

def is_gcs_path(path):
    """Check if the path is a Google Cloud Storage path"""
    return path.startswith('gs://')

def get_file_list(data_dir, file_pattern):
    """Get list of files from either local directory or GCS"""
    if is_gcs_path(data_dir):
        # Initialize GCS filesystem
        fs = gcsfs.GCSFileSystem()
        # List files in GCS bucket
        full_pattern = os.path.join(data_dir, file_pattern)
        files = fs.glob(full_pattern)
        if not files:
            raise ValueError(f"No files found matching pattern: {full_pattern}")
        return [f"gs://{f}" for f in files]
    else:
        # Local file system
        files = glob.glob(os.path.join(data_dir, file_pattern))
        if not files:
            raise ValueError(f"No files found matching pattern: {os.path.join(data_dir, file_pattern)}")
        return files

def get_file_size(file_path):
    """Get file size in GB"""
    if is_gcs_path(file_path):
        fs = gcsfs.GCSFileSystem()
        return fs.size(file_path) / (1024**3)  # Convert to GB
    else:
        return os.path.getsize(file_path) / (1024**3)  # Convert to GB

def setup_dask_cluster():
    """Setup Dask cluster with GPU configuration"""
    # Dask dashboard
    dashboard_port = "8787"

    # Deploy a Single-Machine Multi-GPU Cluster
    protocol = "tcp"  # "tcp" or "ucx"
    if numba.cuda.is_available():
        NUM_GPUS = list(range(len(numba.cuda.gpus)))
    else:
        NUM_GPUS = []
    visible_devices = ",".join([str(n) for n in NUM_GPUS])
    
    # Memory configuration
    device_limit_frac = 0.7  # Spill GPU-Worker memory to host at this limit
    device_pool_frac = 0.8
    # part_mem_frac = part_mem_fraction

    # Calculate memory limits
    device_size = device_mem_size(kind="total")
    device_limit = int(device_limit_frac * device_size)
    device_pool_size = int(device_pool_frac * device_size)
    # part_size = int(part_mem_frac * device_size)

    logging.info(f"visible_devices: {visible_devices}")
    logging.info(f"Device size: {device_size / 1e9:.2f} GB")
    logging.info(f"Device limit: {device_limit / 1e9:.2f} GB")
    logging.info(f"Device pool size: {device_pool_size / 1e9:.2f} GB")
    # logging.info(f"Part size: {part_size / 1e9:.2f} GB")

    # Check if any device memory is already occupied
    for dev in visible_devices.split(","):
        fmem = pynvml_mem_size(kind="free", index=int(dev))
        used = (device_size - fmem) / 1e9
        if used > 1.0:
            warnings.warn(f"BEWARE - {used:.2f} GB is already occupied on device {int(dev)}!")

    # Create and configure the cluster
    cluster = LocalCUDACluster(
        protocol=protocol,
        n_workers=len(visible_devices.split(",")),
        CUDA_VISIBLE_DEVICES=visible_devices,
        device_memory_limit=device_limit,
        dashboard_address=":" + dashboard_port,
        rmm_pool_size=(device_pool_size // 256) * 256
    )

    # Create the distributed client
    client = Client(cluster)
    return client, cluster

def preprocess_data(train_paths, client, vocab_size, part_size):
    """Preprocess the data using NVTabular workflow"""
    # Calculate total data size
    total_size = sum(get_file_size(f) for f in train_paths)
    logging.info(f"Total data size: {total_size:.2f} GB")
    
    # Define the workflow
    logging.info("Setting up workflow...")

    cat_features = (
        CATEGORICAL_COLUMNS
        # >> LambdaOp(lambda x: x)
        >> LambdaOp(lambda col: col.str.hex_to_int() % vocab_size)
        >> Categorify()
    )

    cont_features = (
        CONTINUOUS_COLUMNS 
        # >> LambdaOp(lambda x: x)
        >> Clip(min_value=0)
        >> LogOp()
    )

    features = LABEL_COLUMNS + cont_features + cat_features
    workflow = nvt.Workflow(features, client=client)

    # Create dataset with modified parameters
    logging.info("Creating dataset...")
    try:
        # Try without strings_to_categorical
        train_ds_iterator = nvt.Dataset(
            train_paths,
            engine='parquet',
            part_size=part_size
        )
        logging.info("Dataset created successfully with cuDF engine")
        logging.info(f"Part size: {part_size}")
    except ValueError as e:
        if "strings_to_categorical" in str(e):
            logging.info("Retrying with pandas engine...")
            # Fall back to pandas engine if cuDF fails
            train_ds_iterator = nvt.Dataset(
                train_paths,
                engine='parquet',
                part_size=part_size,
                cpu=True
            )
        else:
            raise e

    # Fit and transform training data
    logging.info('Fitting workflow...')
    start_time = time.time()
    
    # Log initial GPU memory state
    for dev in range(len(numba.cuda.gpus)):
        fmem = pynvml_mem_size(kind="free", index=dev)
        used = (device_mem_size(kind="total") - fmem) / 1e9
        logging.info(f"Initial GPU {dev} memory usage: {used:.2f} GB")
    
    # Fit the workflow
    workflow.fit(train_ds_iterator)
    
    # Log GPU memory after fitting
    for dev in range(len(numba.cuda.gpus)):
        fmem = pynvml_mem_size(kind="free", index=dev)
        used = (device_mem_size(kind="total") - fmem) / 1e9
        logging.info(f"GPU {dev} memory usage after fitting: {used:.2f} GB")
    
    fit_time = time.time() - start_time
    logging.info(f'Workflow fitting completed in {fit_time:.2f} seconds')

    logging.info('Transforming dataset...')
    start_time = time.time()
    
    # Transform the data with progress tracking
    transformed_data = workflow.transform(train_ds_iterator)
    
    # Monitor progress of transformation
    with tqdm(total=len(train_paths), desc="Processing files") as pbar:
        for _ in transformed_data.to_iter():
            pbar.update(1)
            # Log memory usage periodically
            if pbar.n % 10 == 0:  # Log every 10 files
                for dev in range(len(numba.cuda.gpus)):
                    fmem = pynvml_mem_size(kind="free", index=dev)
                    used = (device_mem_size(kind="total") - fmem) / 1e9
                    logging.info(f"GPU {dev} memory usage: {used:.2f} GB")

    transform_time = time.time() - start_time
    logging.info(f'Dataset transformation completed in {transform_time:.2f} seconds')
    
    # Log final statistics
    logging.info(f"Total processing time: {fit_time + transform_time:.2f} seconds")
    logging.info(f"Average processing speed: {total_size / (fit_time + transform_time):.2f} GB/s")

    return

def main():
    parser = argparse.ArgumentParser(description="NVTabular Preprocessing for Criteo Dataset")
    parser.add_argument('--data_dir', type=str, required=True, 
                      help="Directory containing the parquet files (local path or GCS path starting with 'gs://')")
    parser.add_argument('--file_pattern', type=str, default="*.parquet", 
                      help="Pattern to match training parquet files")
    parser.add_argument('--part_size', type=str, default="1GB", 
                      help="Size of each data partition in GB")
    parser.add_argument('--vocab_size', type=int, default=8192, 
                      help="Vocabulary size for categorical features")
    args = parser.parse_args()

    # Start timing
    runtime = time.time()

    # Setup Dask cluster
    client, cluster = setup_dask_cluster()

    try:
        # Get parquet file paths
        train_paths = get_file_list(args.data_dir, args.file_pattern)
        logging.info(f"Found {len(train_paths)} training files")
        if len(train_paths) > 0:
            logging.info(f"First file: {train_paths[0]}")
            logging.info(f"Last file: {train_paths[-1]}")

        # Preprocess data
        preprocess_data(train_paths, client, args.vocab_size, args.part_size)

    finally:
        # Cleanup
        client.shutdown()
        cluster.close()

    # Print runtime
    runtime = time.time() - runtime
    logging.info("\nDask-NVTabular Criteo Preprocessing Done!")
    logging.info(f"Total Runtime[s]: {runtime:.2f}")
    logging.info("======================================\n")

if __name__ == "__main__":
    main()