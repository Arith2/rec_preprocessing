import time
import pandas as pd
import argparse
from tqdm import tqdm
from joblib import Parallel, delayed
import gc
import os
from google.cloud import storage

def read_parquet_chunk(file_path, chunk_size):
    """Read a chunk of parquet file and return time taken"""
    start_time = time.time()
    try:
        df = pd.read_parquet(file_path, chunksize=chunk_size)
        # Read first chunk to ensure actual data transfer
        next(df)
        end_time = time.time()
        return end_time - start_time
    except Exception as e:
        print(f"Error reading {file_path}: {str(e)}")
        return None

def benchmark_gcs_read(bucket_name, prefix, chunk_size, n_jobs=1):
    """Benchmark GCS read performance"""
    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # List all parquet files
    blobs = list(bucket.list_blobs(prefix=prefix))
    parquet_files = [f"gs://{bucket_name}/{blob.name}" for blob in blobs if blob.name.endswith('.parquet')]
    
    print(f"Found {len(parquet_files)} parquet files to benchmark")
    
    # Test sequential read
    print("\nTesting sequential read...")
    total_time = 0
    for file_path in tqdm(parquet_files):
        read_time = read_parquet_chunk(file_path, chunk_size)
        if read_time:
            total_time += read_time
            print(f"File: {os.path.basename(file_path)}, Time: {read_time:.2f}s")
    
    avg_seq_time = total_time / len(parquet_files) if parquet_files else 0
    print(f"\nAverage sequential read time: {avg_seq_time:.2f}s per file")
    
    # Test parallel read
    if n_jobs > 1:
        print(f"\nTesting parallel read with {n_jobs} jobs...")
        start_time = time.time()
        results = Parallel(n_jobs=n_jobs)(
            delayed(read_parquet_chunk)(file_path, chunk_size)
            for file_path in tqdm(parquet_files)
        )
        parallel_time = time.time() - start_time
        avg_parallel_time = parallel_time / len(parquet_files) if parquet_files else 0
        
        print(f"Total parallel read time: {parallel_time:.2f}s")
        print(f"Average parallel read time: {avg_parallel_time:.2f}s per file")
        print(f"Speedup factor: {avg_seq_time/avg_parallel_time:.2f}x")

def main():
    parser = argparse.ArgumentParser(description='Benchmark GCS read performance')
    parser.add_argument('--bucket', required=True, help='GCS bucket name')
    parser.add_argument('--prefix', required=True, help='Prefix for parquet files')
    parser.add_argument('--chunk-size', type=int, default=1000000,
                       help='Number of rows to read at once')
    parser.add_argument('--n-jobs', type=int, default=1,
                       help='Number of parallel jobs for reading')
    
    args = parser.parse_args()
    
    print(f"Benchmarking GCS read performance:")
    print(f"Bucket: {args.bucket}")
    print(f"Prefix: {args.prefix}")
    print(f"Chunk size: {args.chunk_size}")
    print(f"Parallel jobs: {args.n_jobs}")
    
    benchmark_gcs_read(args.bucket, args.prefix, args.chunk_size, args.n_jobs)

if __name__ == "__main__":
    main() 