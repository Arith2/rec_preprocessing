import pyarrow.parquet as pq
import pyarrow.fs as pafs
import argparse

def split_parquet_gcs(input_file, output_dir, num_shards=1024):
    gcs = pafs.GcsFileSystem()
    fs, path = gcs, input_file.replace("gs://", "")

    table = pq.read_table(path, filesystem=fs)
    total_rows = table.num_rows
    rows_per_shard = total_rows // num_shards
    remainder = total_rows % num_shards

    output_path_root = output_dir.replace("gs://", "")

    start = 0
    for i in range(num_shards):
        end = start + rows_per_shard + (1 if i < remainder else 0)
        shard = table.slice(start, end - start)
        shard_path = f"{output_path_root}/shard_{i:04d}.parquet"
        pq.write_table(shard, shard_path, filesystem=fs)
        print(f"Wrote {shard_path} with {shard.num_rows} rows")
        start = end

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", required=True, help="Input Parquet file (GCS URI)")
    parser.add_argument("--output_dir", required=True, help="Output directory (GCS URI)")
    parser.add_argument("--num_shards", type=int, default=1024, help="Number of output files")
    args = parser.parse_args()

    split_parquet_gcs(args.input_file, args.output_dir, args.num_shards)
