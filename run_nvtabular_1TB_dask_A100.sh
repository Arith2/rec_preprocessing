#!/bin/bash

# Set GPU device
export CUDA_VISIBLE_DEVICES=0

# Input directory containing parquet files (can be local or GCS path)
# INPUT_DIR="/local/home/yuzhuyu/criteo_1TB"  # Local path
INPUT_DIR="/mnt/myssd/criteo_1TB"  # A100
# INPUT_DIR="gs://your-bucket/criteo_1TB"  # GCS path example

# Test different part_size values (smaller sizes for better memory management)
PART_SIZES=("512MB")
# VOCAB_SIZE=536870912
VOCAB_SIZE=8192

# Number of runs for each configuration
NUM_RUNS=1

# Function to monitor GPU utilization
monitor_gpu() {
    while true; do
        nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits
        sleep 1
    done
}

# Function to clear GPU memory
clear_gpu_memory() {
    python3 -c "
import torch
torch.cuda.empty_cache()
import gc
gc.collect()
"
}

# Function to setup GCS authentication if needed
setup_gcs_auth() {
    # Only check GCS authentication if using a GCS path
    if [[ "$INPUT_DIR" == gs://* ]]; then
        echo "Using GCS path, make sure you've run 'gcloud auth application-default login'"
    else
        echo "Using local file system, skipping GCS authentication"
    fi
}

# Create logs directory if it doesn't exist
mkdir -p logs

# Setup GCS authentication if needed
setup_gcs_auth

# Run tests for each part_size
for part_size in "${PART_SIZES[@]}"; do
    echo "Testing with part_size = $part_size"
    
    # Run multiple times for each configuration
    for ((run=1; run<=NUM_RUNS; run++)); do
        echo "Run $run of $NUM_RUNS"
        
        # Clear GPU memory before each run
        clear_gpu_memory
        
        # Start GPU monitoring in background
        monitor_gpu > "logs/gpu_util_${part_size}_run${run}.log" &
        MONITOR_PID=$!
        
        # Run the preprocessing script with adjusted memory settings
        echo "Running preprocessing with part_size=$part_size"
        python nvtabular_gpu_1TB_8k_no_vocab_dask.py \
            --data_dir "$INPUT_DIR" \
            --file_pattern "combine_128_part_*.parquet" \
            --part_size "$part_size" \
            --vocab_size "$VOCAB_SIZE" \
            2>&1 | tee "logs/output_${part_size}_run${run}.log"
        
        # Stop GPU monitoring
        kill $MONITOR_PID
        
        # Clear GPU memory after each run
        clear_gpu_memory
        
        # Wait a bit between runs
        sleep 5
    done
done

echo "All tests completed. Check the logs directory for results."
