# Use the official Apache Beam SDK base image
FROM apache/beam_python3.10_sdk:latest

# Set working directory
WORKDIR /opt/apache/beam

# Copy your requirements.txt
COPY requirements.txt ./requirements.txt

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Optional: Copy your Beam code (if packaging it with the image)
# COPY your_script.py .

# Default entrypoint (leave as is for Beam)
ENTRYPOINT ["/opt/apache/beam/boot"]

