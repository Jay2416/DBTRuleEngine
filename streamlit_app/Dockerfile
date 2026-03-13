FROM python:3.9-slim
WORKDIR /app

# Install dependencies required for Streamlit, PyHive (Spark Thrift), and general data handling
COPY requirements.txt .
RUN apt-get update && apt-get install -y gcc g++ libsasl2-dev
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y docker.io

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]