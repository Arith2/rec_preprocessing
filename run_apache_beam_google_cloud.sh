#!/bin/bash

# Set the necessary parameters
STORAGE_BUCKET=gs://criteo_preprocessing
PROJECT=cloud-shared-execution
REGION=europe-west1  # Must match the GCS bucket location
MACHINE_TYPE="n2-standard-16"  # Specify the machine type (e.g., "n2-standard-16")
SDK_CONTAINER_IMAGE=europe-west1-docker.pkg.dev/cloud-shared-execution/beam-docker/beam-custom:latest
NUM_WORKERS=1


DATASET_NAME="criteo_1TB"

JOB_TYPE="no_vocab"
VOCAB_SIZE=8192
JOB_NAME="${DATASET_NAME}-vocab_${VOCAB_SIZE}-workers_${NUM_WORKERS}_${MACHINE_TYPE}_${JOB_TYPE}"
JOB_NAME_CLEAN="${JOB_NAME//_/-}"
JOB_NAME_CLEAN=$(echo "${JOB_NAME_CLEAN}" | tr '[:upper:]' '[:lower:]')

python3 apache_beam_google_cloud_parquet_${JOB_TYPE}.py \
  --input_path "${STORAGE_BUCKET}/${DATASET_NAME}/*.parquet" \
  --output_path "${STORAGE_BUCKET}/${DATASET_NAME}_output/vocab_${VOCAB_SIZE}_workers_${NUM_WORKERS}/" \
  --temp_dir "${STORAGE_BUCKET}/${DATASET_NAME}_temp/vocab_${VOCAB_SIZE}_workers_${NUM_WORKERS}" \
  --runner DataflowRunner --project ${PROJECT} \
  --region ${REGION} \
  --machine_type ${MACHINE_TYPE} \
  --num_workers ${NUM_WORKERS} \
  --sdk_container_image ${SDK_CONTAINER_IMAGE} \
  --job_name "${JOB_NAME_CLEAN}"  \
  --autoscaling_algorithm NONE \
  --max_num_workers ${NUM_WORKERS}


# JOB_TYPE="vocab_small"
# JOB_SUFFIX="gen"
# VOCAB_SIZE=8192
# JOB_NAME="preprocess-${DATASET_NAME}-vocab_${VOCAB_SIZE}-workers_${NUM_WORKERS}_${JOB_TYPE}_${JOB_SUFFIX}"
# JOB_NAME_CLEAN="${JOB_NAME//_/-}"
# JOB_NAME_CLEAN=$(echo "${JOB_NAME_CLEAN}" | tr '[:upper:]' '[:lower:]')

# python3 apache_beam_google_cloud_parquet_${JOB_TYPE}.py \
#   --input_path "${STORAGE_BUCKET}/${DATASET_NAME}/*.parquet" \
#   --output_path "${STORAGE_BUCKET}/${DATASET_NAME}_output/vocab_${VOCAB_SIZE}_workers_${NUM_WORKERS}/" \
#   --temp_dir "${STORAGE_BUCKET}/${DATASET_NAME}_temp/vocab_${VOCAB_SIZE}_workers_${NUM_WORKERS}" \
#   --runner DataflowRunner --project ${PROJECT} \
#   --region ${REGION} \
#   --machine_type ${MACHINE_TYPE} \
#   --num_workers ${NUM_WORKERS} \
#   --sdk_container_image ${SDK_CONTAINER_IMAGE} \
#   --job_name "${JOB_NAME_CLEAN}" \
#   --vocab_gen_mode

# JOB_TYPE="vocab_small"
# JOB_SUFFIX="apply"
# VOCAB_SIZE=8192
# JOB_NAME="preprocess-${DATASET_NAME}-vocab_${VOCAB_SIZE}-workers_${NUM_WORKERS}_${JOB_TYPE}_${JOB_SUFFIX}"
# JOB_NAME_CLEAN="${JOB_NAME//_/-}"
# JOB_NAME_CLEAN=$(echo "${JOB_NAME_CLEAN}" | tr '[:upper:]' '[:lower:]')

# python3 apache_beam_google_cloud_parquet_${JOB_TYPE}.py \
#   --input_path "${STORAGE_BUCKET}/${DATASET_NAME}/*.parquet" \
#   --output_path "${STORAGE_BUCKET}/${DATASET_NAME}_output/vocab_${VOCAB_SIZE}_workers_${NUM_WORKERS}/" \
#   --temp_dir "${STORAGE_BUCKET}/${DATASET_NAME}_temp/vocab_${VOCAB_SIZE}_workers_${NUM_WORKERS}" \
#   --runner DataflowRunner --project ${PROJECT} \
#   --region ${REGION} \
#   --machine_type ${MACHINE_TYPE} \
#   --num_workers ${NUM_WORKERS} \
#   --sdk_container_image ${SDK_CONTAINER_IMAGE} \
#   --job_name "${JOB_NAME_CLEAN}" 
