import pyarrow.parquet as pq
import pyarrow as pa
import os
from tqdm import tqdm

input_folder = "/home/yuzhuyu/criteo_1TB"
output_folder = "/mnt/myssd/criteo_1TB"
output_prefix = "combine_128_part"
files_per_group = 16
total_files = 128
ROW_GROUP_SIZE = 3000000  # 3 million rows per group

# Create output directory if it doesn't exist
os.makedirs(output_folder, exist_ok=True)
print(f"Output directory: {output_folder}")
print(f"Row group size: {ROW_GROUP_SIZE:,} rows")

# Process in batches of 16 files
for group_id in tqdm(range(total_files // files_per_group), desc="Processing groups"):
    start = group_id * files_per_group
    end = start + files_per_group

    # Read and concatenate 16 files
    file_paths = [
        os.path.join(input_folder, f"criteo_1TB_part_{i:04d}.parquet")
        for i in range(start, end)
    ]
    
    print(f"\nProcessing group {group_id + 1}/8 (files {start}-{end-1})")
    print(f"Reading files: {', '.join(os.path.basename(f) for f in file_paths)}")
    
    tables = [pq.read_table(f) for f in file_paths]
    combined_table = pa.concat_tables(tables)

    # Write to output with specified row group size
    output_file = os.path.join(output_folder, f"{output_prefix}_{group_id}.parquet")
    print(f"Writing to: {output_file}")
    print(f"Total rows: {combined_table.num_rows:,}")
    print(f"Number of row groups: {combined_table.num_rows // ROW_GROUP_SIZE + (1 if combined_table.num_rows % ROW_GROUP_SIZE else 0)}")
    
    pq.write_table(combined_table, output_file, row_group_size=ROW_GROUP_SIZE)
    print(f"✅ Written: {output_file} ({combined_table.num_rows:,} rows)")
