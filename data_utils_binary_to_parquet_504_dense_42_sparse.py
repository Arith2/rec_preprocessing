import numpy as np
import pandas as pd
import random
import pyarrow as pa
import pyarrow.parquet as pq

def generate_pseudo_parquet(output_parquet,
                             num_rows=4_000_000,
                             target_column='col_0',
                             num_dense=504,
                             num_sparse=42,
                             row_group_size=100_000):
    # Step 1: Generate the target column with binary values (0 or 1)
    target_data = np.random.randint(0, 2, size=num_rows)

    # Step 2: Generate dense features with random positive and negative float values as float32
    dense_data = np.random.uniform(-1000, 1000, size=(num_rows, num_dense)).astype(np.float32)

    # Step 3: Generate sparse features with random 32-bit hexadecimal values
    sparse_data = np.array([[f"{random.randint(0, 0xFFFFFFFF):08X}" for _ in range(num_sparse)] for _ in range(num_rows)])

    # Step 4: Create a DataFrame and combine all data
    df = pd.DataFrame(target_data, columns=[target_column])

    dense_column_names = [f'col_{i+1}' for i in range(num_dense)]
    dense_df = pd.DataFrame(dense_data, columns=dense_column_names)

    sparse_column_names = [f'col_{i+1+num_dense}' for i in range(num_sparse)]
    sparse_df = pd.DataFrame(sparse_data, columns=sparse_column_names)

    df = pd.concat([df, dense_df, sparse_df], axis=1)

    # Step 5: Write with PyArrow and multiple row groups
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table,
                   output_parquet,
                   compression=None,
                   row_group_size=row_group_size)

    # Step 6: Verify and report row group count
    pf = pq.ParquetFile(output_parquet)
    print(f"✅ Pseudo dataset written to {output_parquet}")
    print(f"   → Rows: {num_rows}")
    print(f"   → Columns: {df.shape[1]}")
    print(f"   → Row groups: {pf.num_row_groups} (row_group_size={row_group_size})")

# Example usage
output_parquet_path = "/mnt/scratch/yuzhuyu/parquet/pseudo_dataset_504_dense_42_sparse.parquet"
generate_pseudo_parquet(output_parquet_path)
