#!/bin/bash

# Set the necessary parameters
STORAGE_BUCKET=gs://criteo_preprocessing
PROJECT=cloud-shared-execution
REGION=europe-west1  # Must match the GCS bucket location
MACHINE_TYPE="n2-standard-16"  # Specify the machine type (e.g., "n2-standard-16")
SDK_CONTAINER_IMAGE=gcr.io/cloud-shared-execution/beam-custom:latest

# Verify bucket location
echo "Verifying bucket location..."
BUCKET_LOCATION=$(gsutil ls -L -b $STORAGE_BUCKET | grep "Location constraint:" | awk '{print $3}')
if [ "${BUCKET_LOCATION,,}" != "${REGION,,}" ]; then
    echo "WARNING: Bucket location ($BUCKET_LOCATION) does not match specified region ($REGION)"
    echo "This may result in reduced performance and higher costs due to cross-region data transfer"
    read -p "Do you want to continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
else
    echo "Bucket location matches specified region"
fi

# Run the Apache Beam pipeline
python3 apache_beam_google_cloud_no_vocab.py \
  --input_path "${STORAGE_BUCKET}/bin2parquet_singleRow.parquet" \
  --output_path "${STORAGE_BUCKET}/criteo_1TB_output/" \
  --temp_dir "${STORAGE_BUCKET}/criteo_1TB_temp/" \
  --vocab_gen_mode \
  --runner DataflowRunner \
  --max_vocab_size 8192 \
  --project ${PROJECT} \
  --region ${REGION} \
  --machine_type ${MACHINE_TYPE} \
  --num_workers 8 \
  --max_num_workers 32 \
  --autoscalingAlgorithm=THROUGHPUT_BASED \
  --sdk_container_image ${SDK_CONTAINER_IMAGE}

