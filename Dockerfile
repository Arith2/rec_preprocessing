FROM python:3.10.0-slim

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
