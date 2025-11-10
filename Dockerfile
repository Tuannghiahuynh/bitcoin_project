FROM apache/airflow:2.7.3-python3.11

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Copy project files
COPY --chown=airflow:root . /opt/airflow/bitcoin_project/

# Set working directory
WORKDIR /opt/airflow

# Add bitcoin_project to PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/bitcoin_project"
