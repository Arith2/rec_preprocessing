#!/bin/bash

# Define the num-threads values
NUM_THREADS_VALUES=(128 128 96 64 32 16)
# Define the number of loops for each function
NUM_LOOPS=1

# Function to run tests
run_tests () {
    # Use the provided script name as the first argument
    file_name="$1"
    # Use modulus values as the second argument
    modulus_values="$2"
    script_name="data_utils_${file_name}_${modulus_values}.py"
    output_file="log_${script_name}.log"

    # Clear the contents of the output file
    > "$output_file"

    # Loop through the different combinations of num-threads and modulus values
    for num_threads in "${NUM_THREADS_VALUES[@]}"; do
        for modulus in $modulus_values; do
            command="python $script_name --n-jobs $num_threads --modulus $modulus"
            echo $command | tee -a "$output_file"

            for ((i=1; i<=NUM_LOOPS; i++)); do
                # Run the command with the current combination of num-threads and modulus
                { $command; } 2>&1 | tee -a "$output_file"    
            done

            # Add a newline for clarity between runs
            echo -e "\n" >> "$output_file"
        done
    done
}

# Example usage with different modulus values
# Run with modulus 8192
run_tests "parquet_vocab_1TB_no" "8192"

# Run with both modulus values
run_tests "parquet_vocab_1TB_gen" "8192 536870912"

# Run the tests with the specified scripts

# run_tests "parquet_vocab_large_gen"
 