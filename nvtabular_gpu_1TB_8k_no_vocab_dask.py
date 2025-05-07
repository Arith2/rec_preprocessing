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

import dask_cudf
from dask_cuda import LocalCUDACluster
from dask.distributed import Client

import nvtabular as nvt
from merlin.core.compat import device_mem_size, pynvml_mem_size
from nvtabular.ops import (
    Categorify,
    Clip,
    FillMissing,
    Normalize,
    get_embedding_sizes,
)

import logging

# Set up logging
logging.basicConfig(format="%(asctime)s %(message)s")
# logging.root.setLevel(logging.NOTSET)
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("numba").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)

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
    part_mem_frac = 0.15

    # Calculate memory limits
    device_size = device_mem_size(kind="total")
    device_limit = int(device_limit_frac * device_size)
    device_pool_size = int(device_pool_frac * device_size)
    part_size = int(part_mem_frac * device_size)

    print("visible_devices: ", visible_devices)
    print(f"Device size: {device_size / 1e9} GB")
    print(f"Device limit: {device_limit / 1e9} GB")
    print(f"Device pool size: {device_pool_size / 1e9} GB")
    print(f"Part size: {part_size / 1e9} GB")

    # Check if any device memory is already occupied
    for dev in visible_devices.split(","):
        fmem = pynvml_mem_size(kind="free", index=int(dev))
        used = (device_size - fmem) / 1e9
        if used > 1.0:
            warnings.warn(f"BEWARE - {used} GB is already occupied on device {int(dev)}!")

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

def preprocess_data(train_paths, client):
    """Preprocess the data using NVTabular workflow"""
    # Define the workflow
    categorify_op = Categorify()
    cat_features = CATEGORICAL_COLUMNS >> categorify_op
    cont_features = CONTINUOUS_COLUMNS >> FillMissing() >> Clip(min_value=0) >> Normalize()

    # features = LABEL_COLUMNS
    # features += cont_features
    # features += cat_features

    features = LABEL_COLUMNS + cont_features + cat_features


    workflow = nvt.Workflow(features, client=client)

    # Create dataset
    train_ds_iterator = nvt.Dataset(train_paths, engine='parquet')

    # Fit and transform training data
    logging.info('Train Dataset Preprocessing.....')
    workflow.fit(train_ds_iterator)
    transformed_data = workflow.transform(train_ds_iterator)


    return

def main():
    parser = argparse.ArgumentParser(description="NVTabular Preprocessing for Criteo Dataset")
    parser.add_argument('--data_dir', type=str, required=True, help="Directory containing the parquet files")
    parser.add_argument('--file_pattern', type=str, default="*.parquet", help="Pattern to match training parquet files")
    parser.add_argument('--part_mem_fraction', type=float, default=0.3, help="Fraction of GPU memory to use for each data partition (0-1)")
    args = parser.parse_args()

    # Start timing
    runtime = time.time()

    # Setup Dask cluster
    client, cluster = setup_dask_cluster()

    try:
        # Get parquet file paths
        train_paths = glob.glob(os.path.join(args.data_dir, args.file_pattern))

        if not train_paths:
            raise ValueError(f"No training files found matching pattern: {args.file_pattern}")

        logging.info(f"Found {len(train_paths)} training files")

        # Preprocess data
        preprocess_data(
            train_paths,
            client
        )

    finally:
        # Cleanup
        client.shutdown()
        cluster.close()

    # Print runtime
    runtime = time.time() - runtime
    print("\nDask-NVTabular Criteo Preprocessing Done!")
    print(f"Runtime[s]         | {runtime}")
    print("======================================\n")

if __name__ == "__main__":
    main()