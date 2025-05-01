import pyarrow.parquet as pq
import time

start = time.time()
table = pq.read_table("gs://criteo_preprocessing/bin2parquet.parquet")
end = time.time()

print(f"Read {table.num_rows} rows in {end - start:.2f} seconds")
