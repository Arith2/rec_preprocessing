STORAGE_BUCKET="/mnt/scratch/yuzhuyu/parquet"

python3 apache_beam_google_cloud_no_vocab.py \
  --input_path="${STORAGE_BUCKET}/criteo_1TB/criteo_1TB_part_1023.parquet" \
  --output_path="${STORAGE_BUCKET}/criteo_1TB_output/" \
  --temp_dir="${STORAGE_BUCKET}/criteo_1TB_temp/" \
  --vocab_gen_mode --runner=DirectRunner --max_vocab_size=8192

# python3 apache_beam_google_cloud.py \
#   --input_path "${STORAGE_BUCKET}/criteo_1TB/criteo_1TB_part_1023.parquet" \
#   --output_path "${STORAGE_BUCKET}/criteo_1TB_output/" \
#   --temp_dir "${STORAGE_BUCKET}/criteo_1TB_temp/" \
#   --vocab_gen_mode --runner DirectRunner --max_vocab_size 8192 \
#   --project ${PROJECT} --region ${REGION}