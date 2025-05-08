import os
import time
import argparse
import glob
from tqdm import tqdm

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
    
    # Calculate total size for progress bar
    total_size = sum(os.path.getsize(f) for f in files)
    total_size_gb = total_size / (1024**3)
    print(f"Total data size: {total_size_gb:.2f} GB")

    start = time.time()
    total_rows = 0
    processed_size = 0
    last_log_time = time.time()

    # Create progress bar
    with tqdm(total=len(files), desc="Reading Parquet files") as pbar:
        for file in files:
            file_start = time.time()
            file_size = os.path.getsize(file)
            
            if USE_CUDF:
                df = cudf.read_parquet(file)
                print("Read with cuDF")
            else:
                df = pd.read_parquet(file, engine=engine)
                print("Read with pandas")
                
            # Update statistics
            total_rows += len(df)
            processed_size += file_size
            
            # Calculate current throughput
            current_time = time.time()
            elapsed = current_time - start
            throughput = processed_size / (1024**3) / elapsed if elapsed > 0 else 0
            
            # Update progress bar
            pbar.update(1)
            pbar.set_postfix({
                'rows': f"{total_rows:,}",
                'size': f"{processed_size/(1024**3):.1f}GB",
                'speed': f"{throughput:.1f}GB/s"
            })
            
            # Log detailed progress every 10 files or 30 seconds
            if pbar.n % 10 == 0 or (current_time - last_log_time) > 30:
                print(f"\nProgress Update:")
                print(f"Files processed: {pbar.n}/{len(files)}")
                print(f"Data processed: {processed_size/(1024**3):.2f} GB / {total_size_gb:.2f} GB")
                print(f"Current throughput: {throughput:.2f} GB/s")
                print(f"Elapsed time: {elapsed:.2f} seconds")
                print(f"File reading time: {current_time - file_start:.2f} seconds")
                last_log_time = current_time

    duration = time.time() - start
    throughput = total_size / (1024 ** 3) / duration  # GB/s

    print(f"\nLoad Summary:")
    print(f"Loaded {len(files)} files")
    print(f"Total rows: {total_rows:,}")
    print(f"Total size: {total_size_gb:.2f} GB")
    print(f"Total time: {duration:.2f} seconds")
    print(f"Average throughput: {throughput:.2f} GB/s")
    print(f"Average time per file: {duration/len(files):.2f} seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", help="Folder containing Parquet files")
    parser.add_argument("--engine", default="auto", help="Pandas engine (if using CPU): 'pyarrow' or 'fastparquet'")
    args = parser.parse_args()

    load_parquet_files(args.folder, engine=args.engine)
