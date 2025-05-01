import pyarrow.parquet as pq
import os
import argparse

def split_parquet(input_file, output_dir, num_shards=1024):
    # Ensure output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Read the full table from the input Parquet file
    table = pq.read_table(input_file)
    total_rows = table.num_rows
    rows_per_shard = total_rows // num_shards
    remainder = total_rows % num_shards

    print(f"Total rows: {total_rows}")
    print(f"Splitting into {num_shards} shards...")

    start = 0
    for i in range(num_shards):
        end = start + rows_per_shard + (1 if i < remainder else 0)
        shard = table.slice(start, end - start)
        shard_path = os.path.join(output_dir, f"shard_{i:04d}.parquet")
        pq.write_table(shard, shard_path)
        print(f"Wrote {shard_path} with {shard.num_rows} rows")
        start = end

    print("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split a local Parquet file into multiple shards.")
    parser.add_argument("--input_file", required=True, help="Path to the input Parquet file")
    parser.add_argument("--output_dir", required=True, help="Directory where shard files will be written")
    parser.add_argument("--num_shards", type=int, default=1024, help="Number of output shards (default: 1024)")
    args = parser.parse_args()

    split_parquet(args.input_file, args.output_dir, args.num_shards)
