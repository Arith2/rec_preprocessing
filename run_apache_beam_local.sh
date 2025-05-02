# STORAGE_BUCKET="/mnt/scratch/yuzhuyu/parquet"
STORAGE_BUCKET="gs://criteo_preprocessing"

python3 apache_beam_google_cloud_parquet.py \
  --input_path="${STORAGE_BUCKET}/criteo_small/shard_1023.parquet" \
  --output_path="${STORAGE_BUCKET}/criteo_small_output/" \
  --temp_dir="${STORAGE_BUCKET}/criteo_small_temp/" \
  --vocab_gen_mode --runner=DirectRunner --max_vocab_size=8192
