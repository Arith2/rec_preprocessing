import os
import time
import argparse
import dask_cudf
from tqdm import tqdm
import fsspec

def get_gcs_parquet_files(gcs_path):
    fs = fsspec.filesystem("gcs")
    files = fs.glob(gcs_path + "/*.parquet")
    if not files:
        raise FileNotFoundError(f"No Parquet files found at: {gcs_path}")
    return ["gcs://" + f for f in files]

def read_with_progress(files, npartitions=None):
    print(f"Loading {len(files)} Parquet files from GCS...")
    start = time.time()

    # Read all files with Dask-cuDF
    ddf = dask_cudf.read_parquet(files, storage_options={"anon": False}, split_row_groups=True)

    # Repartition if needed
    if npartitions:
        ddf = ddf.repartition(npartitions=npartitions)

    # Trigger compute with progress bar
    print("Triggering read and compute with progress tracking...")
    parts = ddf.to_delayed()
    results = []
    with tqdm(total=len(parts), desc="Loading partitions") as pbar:
        for delayed_part in parts:
            df = delayed_part.compute()
            results.append(df)
            pbar.update(1)

    end = time.time()
    print(f"Finished loading {len(results)} partitions in {end - start:.2f} seconds")
    return results

def main():
    parser = argparse.ArgumentParser(description="Read Parquet files from GCS with Dask-cuDF")
    parser.add_argument("--gcs_path", type=str, required=True, help="GCS folder path (e.g., gs://bucket/path)")
    parser.add_argument("--npartitions", type=int, default=None, help="Number of partitions to read into")
    args = parser.parse_args()

    files = get_gcs_parquet_files(args.gcs_path)
    results = read_with_progress(files, npartitions=args.npartitions)

if __name__ == "__main__":
    main()
