import os
import glob
import json
import argparse
import pandas as pd
import numpy as np
from tqdm import tqdm
from collections import defaultdict
from joblib import Parallel, delayed

# ----------------------------------------
# Step 1: Collect unique categorical values
# ----------------------------------------
def collect_unique_values(file_list, columns_pipeline_2, modulus):
    global_uniques = defaultdict(set)

    for file in tqdm(file_list, desc="Collecting unique values"):
        for col in columns_pipeline_2:
            df = pd.read_parquet(file, columns=[col])
            modded_values = df[col].dropna().apply(
                lambda x: int(x, 16) % modulus if isinstance(x, str) else x % modulus
            )
            global_uniques[col].update(modded_values.unique())

    return global_uniques


# ----------------------------------------
# Step 2: Build global vocab mapping
# ----------------------------------------
def build_global_mappings(global_uniques):
    mappings = {}
    for col, values in global_uniques.items():
        sorted_unique = sorted(values)
        mappings[col] = {val: idx for idx, val in enumerate(sorted_unique)}
    return mappings


# ----------------------------------------
# Step 3: Process and transform columns
# ----------------------------------------
def process_column_with_mapping(file, column, mapping, output_dir):
    df = pd.read_parquet(file, columns=[column])
    df[column] = df[column].apply(lambda x: int(x, 16) if isinstance(x, str) else x)
    df[column] = df[column].map(lambda x: mapping.get(x % len(mapping), -1))

    # Save output per column per file (optional format)
    os.makedirs(output_dir, exist_ok=True)
    base = os.path.basename(file).replace(".parquet", f"_{column}.parquet")
    out_path = os.path.join(output_dir, base)
    df.to_parquet(out_path, index=False)

# ----------------------------------------
# Argument parsing and orchestration
# ----------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Global Categorify over Parquet files")
    parser.add_argument("--data-dir", type=str, required=True, help="Folder containing Parquet files")
    parser.add_argument("--file-pattern", type=str, default="*.parquet", help="Glob pattern (default: *.parquet)")
    parser.add_argument("--modulus", type=int, default=8192, help="Hash modulus for categorical values")
    parser.add_argument("--n-jobs", type=int, default=8, help="Number of parallel jobs")
    parser.add_argument("--output-dir", type=str, required=True, help="Folder to write processed columns")
    parser.add_argument("--save-vocab", type=str, default="vocab_mapping.json", help="File to save vocab mapping")

    args = parser.parse_args()

    # Define categorical columns
    columns_pipeline_2 = [f"col_{i}" for i in range(14, 40)]

    # List files
    file_list = glob.glob(os.path.join(args.data_dir, args.file_pattern))
    print(f"Found {len(file_list)} files")

    # Step 1: Collect all unique values
    global_uniques = collect_unique_values(file_list, columns_pipeline_2, args.modulus)

    # Step 2: Build vocab mapping
    vocab_mapping = build_global_mappings(global_uniques, args.modulus)

    # Save vocab mapping
    with open(args.save_vocab, "w") as f:
        json.dump({col: {str(k): v for k, v in mapping.items()} for col, mapping in vocab_mapping.items()}, f)
    print(f"Saved vocab mapping to {args.save_vocab}")

    # Step 3: Apply mapping in parallel
    tasks = [
        delayed(process_column_with_mapping)(file, col, vocab_mapping[col], args.output_dir)
        for file in file_list
        for col in columns_pipeline_2
    ]
    Parallel(n_jobs=args.n_jobs)(tasks)

    print("✅ All files processed.")

if __name__ == "__main__":
    main()
