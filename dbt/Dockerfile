FROM python:3.10-slim

WORKDIR /usr/app/dbt

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install dbt with Spark adapter and Thrift drivers
RUN pip install --upgrade pip && \
    pip install dbt-spark[iceberg,pyodbc]==1.5.2 && \
    pip install dbt-core==1.5.2 && \
    pip install pyhive thrift_sasl pure-sasl

# Keep container running
CMD ["tail", "-f", "/dev/null"]
