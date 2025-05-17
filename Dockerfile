# Use the official Airflow image as base
FROM apache/airflow:3.0.1

# Copy requirements.txt and install packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Embed dags
COPY ./dags /opt/airflow/dags
