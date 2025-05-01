FROM python:3.10-slim

# Install cloud-init via apt (NOT pip)
RUN apt-get update && \
    apt-get install -y cloud-init && \
    apt-get clean

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
