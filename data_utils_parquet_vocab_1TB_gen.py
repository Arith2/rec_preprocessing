from joblib import Parallel, delayed
import pandas as pd
import numpy as np
import time
from tqdm import tqdm
import argparse
import os
import glob
from collections import defaultdict

# Argument parser
parser = argparse.ArgumentParser(description='Benchmark for rec_preprocessing')
parser.add_argument('--n-jobs', default=8, type=int, metavar='N',
                    help='number of total jobs to run')
parser.add_argument('--modulus', default=8192, type=int, metavar='M',
                    help='modulus value for sparse features')
parser.add_argument('--output-dir', default='processed_data',
                    help='directory to save processed data')

# Path to your Parquet files directory
parquet_dir = "/mnt/scratch/yuzhuyu/parquet/criteo_1TB"

# Get list of all parquet files
parquet_files = sorted(glob.glob(os.path.join(parquet_dir, "*.parquet")))
print(f"Found {len(parquet_files)} parquet files to process")

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def process_dense_features(file_path):
    # Read dense columns
    dense_columns = [f"col_{i}" for i in range(1, 14)]
    df = pd.read_parquet(file_path, columns=dense_columns)
    
    # Process dense features
    df[df < 0] = 0
    df = df.applymap(lambda x: np.log(x + 1))
    
    return df

def collect_sparse_vocabulary(file_path, modulus):
    # Read sparse columns
    sparse_columns = [f"col_{i}" for i in range(14, 40)]
    df = pd.read_parquet(file_path, columns=sparse_columns)
    
    # Convert to int and apply modulus
    for col in sparse_columns:
        df[col] = df[col].apply(lambda x: int(x, 16) % modulus if isinstance(x, str) else x % modulus)
    
    # Collect unique values
    vocab = {col: set(df[col].unique()) for col in sparse_columns}
    return vocab

def map_sparse_features(file_path, vocab_mapping):
    # Read sparse columns
    sparse_columns = [f"col_{i}" for i in range(14, 40)]
    df = pd.read_parquet(file_path, columns=sparse_columns)
    
    # Map values to indices
    for col in sparse_columns:
        df[col] = df[col].apply(lambda x: vocab_mapping[col].get(x, 0))
    
    return df

def main():
    args = parser.parse_args()
    modulus = args.modulus
    
    # Create output directory
    ensure_dir(args.output_dir)
    
    start_time = time.time()
    
    # First pass: Process dense features and collect sparse vocabulary
    print("First pass: Processing dense features and collecting sparse vocabulary...")
    
    # Process dense features in parallel
    dense_results = Parallel(n_jobs=args.n_jobs)(
        delayed(process_dense_features)(file_path) for file_path in tqdm(parquet_files)
    )
    
    # Collect sparse vocabulary in parallel
    vocab_results = Parallel(n_jobs=args.n_jobs)(
        delayed(collect_sparse_vocabulary)(file_path, modulus) for file_path in tqdm(parquet_files)
    )
    
    # Combine vocabularies from all files
    print("Combining vocabularies...")
    combined_vocab = defaultdict(set)
    for vocab in vocab_results:
        for col, values in vocab.items():
            combined_vocab[col].update(values)
    
    # Create final vocabulary mapping
    vocab_mapping = {}
    for col in combined_vocab:
        # Sort values to ensure consistent ordering
        sorted_values = sorted(combined_vocab[col])
        vocab_mapping[col] = {val: idx for idx, val in enumerate(sorted_values)}
    
    # Save vocabulary
    vocab_dir = os.path.join(args.output_dir, 'vocab')
    ensure_dir(vocab_dir)
    for col, mapping in vocab_mapping.items():
        vocab_df = pd.DataFrame(list(mapping.items()), columns=['value', 'index'])
        vocab_df.to_parquet(os.path.join(vocab_dir, f'{col}_vocab.parquet'))
    
    # Second pass: Map sparse features using vocabulary
    print("Second pass: Mapping sparse features...")
    sparse_results = Parallel(n_jobs=args.n_jobs)(
        delayed(map_sparse_features)(file_path, vocab_mapping) for file_path in tqdm(parquet_files)
    )
    
    # Save processed data
    print("Saving processed data...")
    for i, (dense_df, sparse_df) in enumerate(zip(dense_results, sparse_results)):
        # Combine dense and sparse features
        processed_df = pd.concat([dense_df, sparse_df], axis=1)
        
        # Save to output directory
        output_path = os.path.join(args.output_dir, f'processed_{i:04d}.parquet')
        processed_df.to_parquet(output_path)
    
    # Record the end time and calculate the total execution time
    total_time = time.time() - start_time
    print(f"\nTotal execution time: {total_time:.2f} seconds")
    print(f"Processed {len(parquet_files)} files")
    print(f"Output saved in: {args.output_dir}")
    print(f"Number of jobs used: {args.n_jobs}")
    print(f"Modulus value used: {modulus}")

if __name__ == "__main__":
    main()
