import pyarrow.parquet as pq
import pyarrow as pa
import glob
import math

# Source and output paths
input_folder = "/home/yuzhuyu/criteo_1TB"
output_folder = "/mnt/myssd/criteo_1TB"
output_prefix = "combine_16_part"

# Get list of files to combine
file_paths = [
    f"{input_folder}/criteo_1TB_part_{i:04d}.parquet" for i in range(128)
]

# Read and concatenate all tables
tables = [pq.read_table(f) for f in file_paths]
combined_table = pa.concat_tables(tables)

# Split into 8 equal parts
num_parts = 8
rows_per_part = math.ceil(combined_table.num_rows / num_parts)

for i in range(num_parts):
    start = i * rows_per_part
    end = min((i + 1) * rows_per_part, combined_table.num_rows)
    chunk = combined_table.slice(start, end - start)
    output_file = f"{output_folder}/{output_prefix}_{i}.parquet"
    pq.write_table(chunk, output_file)
    print(f"Wrote {output_file} with {chunk.num_rows} rows")
