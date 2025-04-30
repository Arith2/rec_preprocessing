import numpy as np
import pandas as pd
import random
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import gc

def generate_single_parquet(output_parquet, num_rows=4100000, target_column='col_0', num_dense=13, num_sparse=26):
    try:
        # Initialize empty lists to store data
        target_data = []
        dense_data = []
        sparse_data = []
        
        # Calculate number of chunks for progress reporting
        chunk_size = 100
        num_chunks = num_rows // chunk_size
        
        # Step 1: Generate the target column with binary values (0 or 1)
        print(f"\nGenerating {output_parquet}...")
        for i in tqdm(range(num_chunks), desc="Generating rows"):
            # Generate chunk of target data
            chunk_target = np.random.randint(0, 2, size=chunk_size)
            target_data.extend(chunk_target)
            
            # Generate chunk of dense features
            chunk_dense = np.random.uniform(-1000, 1000, size=(chunk_size, num_dense)).astype(np.float32)
            dense_data.extend(chunk_dense)
            
            # Generate chunk of sparse features
            chunk_sparse = np.array([[f"{random.randint(0, 0xFFFFFFFF):08X}" for _ in range(num_sparse)] for _ in range(chunk_size)])
            sparse_data.extend(chunk_sparse)
            
            # # Print progress every 100 rows
            # if (i + 1) % 10 == 0:  # Print every 1000 rows (10 chunks)
            #     print(f"Processed {(i + 1) * chunk_size:,} rows out of {num_rows:,} rows")
        
        # Convert lists to numpy arrays
        target_data = np.array(target_data)
        dense_data = np.array(dense_data)
        sparse_data = np.array(sparse_data)

        # Step 4: Create a DataFrame and combine all data
        # Target column
        df = pd.DataFrame(target_data, columns=[target_column])

        # Dense features
        dense_column_names = [f'col_{i+1}' for i in range(num_dense)]
        dense_df = pd.DataFrame(dense_data, columns=dense_column_names)

        # Sparse features
        sparse_column_names = [f'col_{i+1+num_dense}' for i in range(num_sparse)]
        sparse_df = pd.DataFrame(sparse_data, columns=sparse_column_names)

        # Concatenate all columns together
        df = pd.concat([df, dense_df, sparse_df], axis=1)

        # Step 5: Write the DataFrame to a Parquet file without compression
        print(f"Writing to parquet file: {output_parquet}")
        df.to_parquet(output_parquet, engine='pyarrow', compression=None)
        print(f"Finished writing to parquet file: {output_parquet}")
        
        # Clean up memory
        del df, dense_df, sparse_df, target_data, dense_data, sparse_data
        gc.collect()
        
        return True, output_parquet
    except Exception as e:
        return False, f"Error processing {output_parquet}: {str(e)}"

def process_single_file(args):
    output_path, rows_per_file = args
    return generate_single_parquet(output_path, rows_per_file)

def generate_criteo_1TB(output_dir, num_files=1024, rows_per_file=4100000, batch_size=8):
    """
    Generate Criteo 1TB dataset split into smaller files
    
    Parameters:
    -----------
    output_dir : str
        Directory to save the parquet files
    num_files : int
        Total number of files to generate (default: 1024)
    rows_per_file : int
        Number of rows per file (default: 4.1M)
    batch_size : int
        Number of concurrent processes (default: 8)
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Calculate total rows and dataset size
    total_rows = num_files * rows_per_file
    bytes_per_row = 1 + (13 * 4) + (26 * 8)  # 1 byte target + 13 float32 + 26 sparse
    total_size_gb = (total_rows * bytes_per_row) / (1024**3)
    
    print(f"Generating {num_files} files with {rows_per_file:,} rows each")
    print(f"Total rows: {total_rows:,}")
    print(f"Estimated dataset size: {total_size_gb:.2f} GB")
    print(f"Using {batch_size} concurrent processes")
    
    # Prepare arguments for parallel processing
    args = []
    for i in range(154, num_files):
        output_path = os.path.join(output_dir, f'criteo_1TB_part_{i:04d}.parquet')
        args.append((output_path, rows_per_file))
    
    # Process files in batches
    completed_files = 0
    failed_files = []
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=batch_size) as executor:
        # Submit all tasks
        future_to_args = {executor.submit(process_single_file, arg): arg for arg in args}
        
        # Process results as they complete
        with tqdm(total=num_files, desc="Generating Criteo 1TB dataset") as pbar:
            for future in as_completed(future_to_args):
                success, result = future.result()
                if success:
                    completed_files += 1
                    pbar.update(1)
                    elapsed_time = time.time() - start_time
                    files_per_second = completed_files / elapsed_time
                    estimated_remaining = (num_files - completed_files) / files_per_second
                    print(f"\nCompleted: {result}")
                    print(f"Progress: {completed_files}/{num_files} files")
                    print(f"Elapsed time: {elapsed_time/3600:.2f} hours")
                    print(f"Estimated remaining time: {estimated_remaining/3600:.2f} hours")
                else:
                    failed_files.append(result)
                    print(f"\nFailed: {result}")
                
                # Add a small delay between batches to prevent overwhelming the system
                if completed_files % batch_size == 0:
                    time.sleep(1)
                    gc.collect()

    total_time = time.time() - start_time
    print(f"\nGeneration Summary:")
    print(f"Total files: {num_files}")
    print(f"Successfully completed: {completed_files}")
    print(f"Failed: {len(failed_files)}")
    print(f"Total time: {total_time/3600:.2f} hours")
    print(f"Average time per file: {total_time/num_files:.2f} seconds")
    if failed_files:
        print("\nFailed files:")
        for failure in failed_files:
            print(failure)

# Example usage with recommended parameters
output_dir = "/mnt/scratch/yuzhuyu/parquet/criteo_1TB"
generate_criteo_1TB(output_dir, num_files=1024, rows_per_file=4100000, batch_size=8)
