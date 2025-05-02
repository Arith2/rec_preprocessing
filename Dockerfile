FROM apache/beam_python3.10_sdk

# Install system dependencies for PyArrow
RUN apt-get update && apt-get install -y --no-install-recommends \
    libatlas-base-dev \
    libhdf5-dev \
    libssl-dev \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Explicitly install GCS connector (in case it's not in requirements)
RUN pip install --no-cache-dir 'apache-beam[gcp]' google-cloud-storage

# # Verify PyArrow installation
# RUN python -c "import pyarrow; import pyarrow.parquet; print('PyArrow version:', pyarrow.__version__)"

# # Set up application
# WORKDIR /app
# COPY apache_beam_google_cloud_parquet.py .

# # Set the entrypoint 
# ENTRYPOINT ["python", "apache_beam_google_cloud_parquet.py"]
