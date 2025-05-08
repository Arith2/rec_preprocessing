import pyarrow.parquet as pq
import pyarrow as pa
import glob
import time
from tqdm import tqdm
import os

# Specify the directory
folder = "/home/yuzhuyu/criteo_1TB"

# Get list of files to combine
file_paths = [
    f"{folder}/criteo_1TB_part_{i:04d}.parquet" for i in range(128)
]

print(f"Found {len(file_paths)} files to combine")

# Calculate total size for progress tracking
total_size = sum(os.path.getsize(f) for f in file_paths)
total_size_gb = total_size / (1024**3)
print(f"Total data size: {total_size_gb:.2f} GB")

start_time = time.time()
processed_size = 0
last_log_time = time.time()

# Read and concatenate with progress tracking
print("\nReading and combining files...")
tables = []
with tqdm(total=len(file_paths), desc="Processing files") as pbar:
    for file in file_paths:
        file_start = time.time()
        file_size = os.path.getsize(file)
        
        # Read the parquet file
        table = pq.read_table(file)
        tables.append(table)
        
        # Update statistics
        processed_size += file_size
        
        # Calculate current throughput
        current_time = time.time()
        elapsed = current_time - start_time
        throughput = processed_size / (1024**3) / elapsed if elapsed > 0 else 0
        
        # Update progress bar
        pbar.update(1)
        pbar.set_postfix({
            'size': f"{processed_size/(1024**3):.1f}GB",
            'speed': f"{throughput:.1f}GB/s"
        })
        
        # Log detailed progress every 10 files or 30 seconds
        if pbar.n % 10 == 0 or (current_time - last_log_time) > 30:
            print(f"\nProgress Update:")
            print(f"Files processed: {pbar.n}/{len(file_paths)}")
            print(f"Data processed: {processed_size/(1024**3):.2f} GB / {total_size_gb:.2f} GB")
            print(f"Current throughput: {throughput:.2f} GB/s")
            print(f"Elapsed time: {elapsed:.2f} seconds")
            print(f"File reading time: {current_time - file_start:.2f} seconds")
            last_log_time = current_time

print("\nCombining tables...")
combine_start = time.time()
combined_table = pa.concat_tables(tables)
combine_time = time.time() - combine_start
print(f"Tables combined in {combine_time:.2f} seconds")

# Write combined file
print("\nWriting combined file...")
write_start = time.time()
pq.write_table(combined_table, f"/mnt/myssd/criteo_1TB/combine_128.parquet")
write_time = time.time() - write_start

# Print final statistics
total_time = time.time() - start_time
print("\nOperation Summary:")
print(f"Total files processed: {len(file_paths)}")
print(f"Total data size: {total_size_gb:.2f} GB")
print(f"Total processing time: {total_time:.2f} seconds")
print(f"Average throughput: {total_size_gb/total_time:.2f} GB/s")
print(f"Time spent combining tables: {combine_time:.2f} seconds")
print(f"Time spent writing output: {write_time:.2f} seconds")
print(f"Combined file written to: /mnt/myssd/criteo_1TB/combine_128.parquet")
