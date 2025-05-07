#!/bin/bash

# Set the necessary parameters
STORAGE_BUCKET=gs://criteo_preprocessing
PROJECT=cloud-shared-execution
REGION=europe-west1  # Must match the GCS bucket location
SDK_CONTAINER_IMAGE=europe-west1-docker.pkg.dev/cloud-shared-execution/beam-docker/beam-custom:v1

# Accept command-line arguments that override environment variables
# Usage: script.sh [MACHINE_TYPE] [NUM_WORKERS] [DATASET_NAME] [JOB_TYPE] [VOCAB_SIZE]
MACHINE_TYPE=${1:-${MACHINE_TYPE:-"n2-standard-64"}}
NUM_WORKERS=${2:-${NUM_WORKERS:-2}}
DATASET_NAME=${3:-${DATASET_NAME:-"criteo_1TB"}}
JOB_TYPE=${4:-${JOB_TYPE:-"vocab"}}
VOCAB_SIZE=${5:-${VOCAB_SIZE:-8192}}

JOB_NAME="${DATASET_NAME}-vocab_${VOCAB_SIZE}-workers_${NUM_WORKERS}_${MACHINE_TYPE}_${JOB_TYPE}_apply"
JOB_NAME_CLEAN="${JOB_NAME//_/-}"
JOB_NAME_CLEAN=$(echo "${JOB_NAME_CLEAN}" | tr '[:upper:]' '[:lower:]')

echo "Running vocab apply with:"
echo "MACHINE_TYPE: $MACHINE_TYPE"
echo "NUM_WORKERS: $NUM_WORKERS"
echo "DATASET_NAME: $DATASET_NAME"
echo "JOB_TYPE: $JOB_TYPE"
echo "VOCAB_SIZE: $VOCAB_SIZE"

# Build the command
CMD="python3 apache_beam_google_cloud_parquet_${JOB_TYPE}.py \\
  --input_path \"${STORAGE_BUCKET}/${DATASET_NAME}/*.parquet\" \\
  --output_path \"${STORAGE_BUCKET}/${DATASET_NAME}_output/vocab_${VOCAB_SIZE}_${NUM_WORKERS}_${MACHINE_TYPE}_${JOB_TYPE}/\" \\
  --temp_dir \"${STORAGE_BUCKET}/${DATASET_NAME}_temp/vocab_${VOCAB_SIZE}_${NUM_WORKERS}_${MACHINE_TYPE}_${JOB_TYPE}\" \\
  --runner DataflowRunner --project ${PROJECT} \\
  --region ${REGION} \\
  --machine_type ${MACHINE_TYPE} \\
  --num_workers ${NUM_WORKERS} \\
  --sdk_container_image ${SDK_CONTAINER_IMAGE} \\
  --job_name \"${JOB_NAME_CLEAN}\"  \\
  --autoscaling_algorithm NONE \\
  --max_num_workers ${NUM_WORKERS} \\
  --max_vocab_size ${VOCAB_SIZE}"

# Echo the command
echo "Executing the following command:"
echo "$CMD"
echo ""

# Execute the command
python3 apache_beam_google_cloud_parquet_${JOB_TYPE}.py \
  --input_path "${STORAGE_BUCKET}/${DATASET_NAME}/*.parquet" \
  --output_path "${STORAGE_BUCKET}/${DATASET_NAME}_output/vocab_${VOCAB_SIZE}_${NUM_WORKERS}_${MACHINE_TYPE}_${JOB_TYPE}/" \
  --temp_dir "${STORAGE_BUCKET}/${DATASET_NAME}_temp/vocab_${VOCAB_SIZE}_${NUM_WORKERS}_${MACHINE_TYPE}_${JOB_TYPE}" \
  --runner DataflowRunner --project ${PROJECT} \
  --region ${REGION} \
  --machine_type ${MACHINE_TYPE} \
  --num_workers ${NUM_WORKERS} \
  --sdk_container_image ${SDK_CONTAINER_IMAGE} \
  --job_name "${JOB_NAME_CLEAN}"  \
  --autoscaling_algorithm NONE \
  --max_num_workers ${NUM_WORKERS} \
  --max_vocab_size ${VOCAB_SIZE} 
 