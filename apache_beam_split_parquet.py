import pyarrow.parquet as pq
import os
import argparse

def split_parquet(input_file, output_dir, num_shards=1024):
    os.makedirs(output_dir, exist_ok=True)

    # Read full table
    table = pq.read_table(input_file)
    total_rows = table.num_rows
    rows_per_shard = total_rows // num_shards
    remainder = total_rows % num_shards

    print(f"Total rows: {total_rows}, Rows per shard: {rows_per_shard}, Remainder: {remainder}")

    start = 0
    for i in range(num_shards):
        end = start + rows_per_shard + (1 if i < remainder else 0)
        shard = table.slice(start, end - start)
        output_path = os.path.join(output_dir, f"shard_{i:04d}.parquet")
        pq.write_table(shard, output_path)
        print(f"Wrote {output_path} with {shard.num_rows} rows")
        start = end

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split a Parquet file into multiple shards.")
    parser.add_argument("--input_file", type=str, required=True, help="Path to input Parquet file")
    parser.add_argument("--output_dir", type=str, required=True, help="Directory to save output shards")
    parser.add_argument("--num_shards", type=int, default=1024, help="Number of shards to split into (default: 1024)")
    args = parser.parse_args()

    split_parquet(args.input_file, args.output_dir, args.num_shards)
