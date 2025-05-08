import pyarrow.parquet as pq
import pyarrow as pa
import os

input_folder = "/home/yuzhuyu/criteo_1TB"
output_folder = "/mnt/myssd/criteo_1TB"
output_prefix = "combine_128_part"
files_per_group = 16
total_files = 128

# Process in batches of 16 files
for group_id in range(total_files // files_per_group):
    start = group_id * files_per_group
    end = start + files_per_group

    # Read and concatenate 16 files
    file_paths = [
        os.path.join(input_folder, f"criteo_1TB_part_{i:04d}.parquet")
        for i in range(start, end)
    ]
    tables = [pq.read_table(f) for f in file_paths]
    combined_table = pa.concat_tables(tables)

    # Write to output
    output_file = os.path.join(output_folder, f"{output_prefix}_{group_id}.parquet")
    pq.write_table(combined_table, output_file)
    print(f"✅ Written: {output_file} ({combined_table.num_rows} rows)")
