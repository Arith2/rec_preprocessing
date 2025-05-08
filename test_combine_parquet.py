import pyarrow.parquet as pq
import pyarrow as pa
import glob

# Specify the directory
folder = "/home/yuzhuyu/criteo_1TB"

# Get list of files to combine
file_paths = [
    f"{folder}/criteo_1TB_part_{i:04d}.parquet" for i in range(128)
]

# Read and concatenate
tables = [pq.read_table(f) for f in file_paths]
combined_table = pa.concat_tables(tables)

# Write combined file
pq.write_table(combined_table, f"/mnt/myssd/criteo_1TB/combine_128.parquet")
print("Combined file written to combine_128.parquet")
