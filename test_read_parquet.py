import os
import time
import argparse
import glob

# Choose either cudf (GPU) or pandas (CPU)
try:
    import cudf
    USE_CUDF = True
    print("Using cuDF for reading Parquet files")
except ImportError:
    import pandas as pd
    USE_CUDF = False
    print("Using pandas for reading Parquet files")

def load_parquet_files(folder, engine="auto"):
    files = glob.glob(os.path.join(folder, "*.parquet"))
    if not files:
        raise FileNotFoundError("No Parquet files found in the folder")

    print(f"Found {len(files)} files. Starting load...")

    start = time.time()
    total_rows = 0
    total_bytes = 0

    for file in files:
        if USE_CUDF:
            df = cudf.read_parquet(file)
        else:
            df = pd.read_parquet(file, engine=engine)

        total_rows += len(df)
        total_bytes += os.path.getsize(file)

    duration = time.time() - start
    throughput = total_bytes / (1024 ** 3) / duration  # GB/s

    print(f"\nLoaded {len(files)} files")
    print(f"Total rows: {total_rows}")
    print(f"Total size: {total_bytes / (1024**3):.2f} GB")
    print(f"Total time: {duration:.2f} s")
    print(f"Throughput: {throughput:.2f} GB/s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", help="Folder containing Parquet files")
    parser.add_argument("--engine", default="auto", help="Pandas engine (if using CPU): 'pyarrow' or 'fastparquet'")
    args = parser.parse_args()

    load_parquet_files(args.folder, engine=args.engine)
