#!/bin/bash

# Set the necessary parameters
STORAGE_BUCKET=gs://criteo_preprocessing
PROJECT=cloud-shared-execution
REGION=europe-west2
MACHINE_TYPE="n2-standard-16"  # Specify the machine type (e.g., "n2-standard-16")
SDK_CONTAINER_IMAGE=gcr.io/cloud-shared-execution/beam-custom:latest


# Run the Apache Beam pipeline
python3 apache_beam_google_cloud_no_vocab.py \
  --input_path "${STORAGE_BUCKET}/criteo_small/*" \
  --output_path "${STORAGE_BUCKET}/criteo_1TB_output/" \
  --temp_dir "${STORAGE_BUCKET}/criteo_1TB_temp/" \
  --vocab_gen_mode \
  --runner DataflowRunner \
  --max_vocab_size 8192 \
  --project ${PROJECT} \
  --region ${REGION} \
  --machine_type ${MACHINE_TYPE} \
  --num_workers 1 \
  --sdk_container_image ${SDK_CONTAINER_IMAGE}

