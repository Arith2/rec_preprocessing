#!/bin/bash

# Default dataset settings (common across all runs)
DATASET_NAME="criteo_1TB"
JOB_TYPE="vocab"
VOCAB_SIZE=536870912

echo "===== RUNNING ALL VOCABULARY GENERATION JOBS ====="

echo "--- GENERATION CONFIG 1: n2-standard-128 with 1 worker ---"
MACHINE_TYPE="n2-standard-128"
NUM_WORKERS=1
# Run generation job for config 1
GEN_COMMAND="bash run_apache_beam_google_cloud_vocab_gen.sh \"$MACHINE_TYPE\" \"$NUM_WORKERS\" \"$DATASET_NAME\" \"$JOB_TYPE\" \"$VOCAB_SIZE\""
echo "Will execute the following command:"
echo "$GEN_COMMAND"
echo ""
echo "Starting vocabulary generation job..."
eval $GEN_COMMAND
# Check if the job completed successfully
if [ $? -ne 0 ]; then
  echo "Vocabulary generation job failed. Exiting."
  exit 1
fi
echo "Generation config 1 completed successfully."
echo ""

echo "--- GENERATION CONFIG 2: n2-standard-64 with 2 workers ---"
MACHINE_TYPE="n2-standard-64"
NUM_WORKERS=2
# Run generation job for config 2
GEN_COMMAND="bash run_apache_beam_google_cloud_vocab_gen.sh \"$MACHINE_TYPE\" \"$NUM_WORKERS\" \"$DATASET_NAME\" \"$JOB_TYPE\" \"$VOCAB_SIZE\""
echo "Will execute the following command:"
echo "$GEN_COMMAND"
echo ""
echo "Starting vocabulary generation job..."
eval $GEN_COMMAND
# Check if the job completed successfully
if [ $? -ne 0 ]; then
  echo "Vocabulary generation job failed. Exiting."
  exit 1
fi
echo "Generation config 2 completed successfully."
echo ""

echo "--- GENERATION CONFIG 3: n2-standard-32 with 4 workers ---"
MACHINE_TYPE="n2-standard-32"
NUM_WORKERS=4
# Run generation job for config 2
GEN_COMMAND="bash run_apache_beam_google_cloud_vocab_gen.sh \"$MACHINE_TYPE\" \"$NUM_WORKERS\" \"$DATASET_NAME\" \"$JOB_TYPE\" \"$VOCAB_SIZE\""
echo "Will execute the following command:"
echo "$GEN_COMMAND"
echo ""
echo "Starting vocabulary generation job..."
eval $GEN_COMMAND
# Check if the job completed successfully
if [ $? -ne 0 ]; then
  echo "Vocabulary generation job failed. Exiting."
  exit 1
fi
echo "Generation config 2 completed successfully."
echo ""

echo "--- GENERATION CONFIG 4: n2-standard-16 with 8 workers ---"
MACHINE_TYPE="n2-standard-16"
NUM_WORKERS=8
# Run generation job for config 3
GEN_COMMAND="bash run_apache_beam_google_cloud_vocab_gen.sh \"$MACHINE_TYPE\" \"$NUM_WORKERS\" \"$DATASET_NAME\" \"$JOB_TYPE\" \"$VOCAB_SIZE\""
echo "Will execute the following command:"
echo "$GEN_COMMAND"
echo ""
echo "Starting vocabulary generation job..."
eval $GEN_COMMAND
# Check if the job completed successfully
if [ $? -ne 0 ]; then
  echo "Vocabulary generation job failed. Exiting."
  exit 1
fi
echo "Generation config 3 completed successfully."
echo ""
echo "All vocabulary generation jobs completed."
echo "Waiting 60 seconds before starting vocabulary apply jobs..."
sleep 60  # Wait a minute to ensure all resources are properly released
echo ""

echo "===== RUNNING ALL VOCABULARY APPLY JOBS ====="
echo "--- APPLY CONFIG 1: n2-standard-128 with 1 worker ---"
MACHINE_TYPE="n2-standard-128"
NUM_WORKERS=1
# Run apply job for config 1
APPLY_COMMAND="bash run_apache_beam_google_cloud_vocab_apply.sh \"$MACHINE_TYPE\" \"$NUM_WORKERS\" \"$DATASET_NAME\" \"$JOB_TYPE\" \"$VOCAB_SIZE\""
echo "Will execute the following command:"
echo "$APPLY_COMMAND"
echo ""
echo "Starting vocabulary apply job..."
eval $APPLY_COMMAND
if [ $? -ne 0 ]; then
  echo "Vocabulary apply job failed. Exiting."
  exit 1
fi
echo "Apply config 1 completed successfully."
echo ""

echo "--- APPLY CONFIG 2: n2-standard-64 with 2 workers ---"
MACHINE_TYPE="n2-standard-64"
NUM_WORKERS=2
# Run apply job for config 2
APPLY_COMMAND="bash run_apache_beam_google_cloud_vocab_apply.sh \"$MACHINE_TYPE\" \"$NUM_WORKERS\" \"$DATASET_NAME\" \"$JOB_TYPE\" \"$VOCAB_SIZE\""
echo "Will execute the following command:"
echo "$APPLY_COMMAND"
echo ""
echo "Starting vocabulary apply job..."
eval $APPLY_COMMAND
if [ $? -ne 0 ]; then
  echo "Vocabulary apply job failed. Exiting."
  exit 1
fi
echo "Apply config 2 completed successfully."
echo ""

echo "--- APPLY CONFIG 3: n2-standard-32 with 4 workers ---"
MACHINE_TYPE="n2-standard-32"
NUM_WORKERS=4
# Run apply job for config 3
APPLY_COMMAND="bash run_apache_beam_google_cloud_vocab_apply.sh \"$MACHINE_TYPE\" \"$NUM_WORKERS\" \"$DATASET_NAME\" \"$JOB_TYPE\" \"$VOCAB_SIZE\""
echo "Will execute the following command:"
echo "$APPLY_COMMAND"
echo ""
echo "Starting vocabulary apply job..."
eval $APPLY_COMMAND
if [ $? -ne 0 ]; then
  echo "Vocabulary apply job failed. Exiting."
  exit 1
fi
echo "Apply config 3 completed successfully."
echo ""

echo "--- APPLY CONFIG 4: n2-standard-16 with 8 workers ---"
MACHINE_TYPE="n2-standard-16"
NUM_WORKERS=8
# Run apply job for config 4
APPLY_COMMAND="bash run_apache_beam_google_cloud_vocab_apply.sh \"$MACHINE_TYPE\" \"$NUM_WORKERS\" \"$DATASET_NAME\" \"$JOB_TYPE\" \"$VOCAB_SIZE\""
echo "Will execute the following command:"
echo "$APPLY_COMMAND"
echo ""
echo "Starting vocabulary apply job..."
eval $APPLY_COMMAND
if [ $? -ne 0 ]; then
  echo "Vocabulary apply job failed. Exiting."
  exit 1
fi
echo "Apply config 4 completed successfully."
echo ""
echo "All vocabulary apply jobs completed successfully!" 