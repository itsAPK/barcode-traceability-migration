FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for pyodbc
RUN apt-get update && apt-get install -y \
    gcc \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir tenacity requests pymysql pyodbc python-dotenv

COPY migration.py .

CMD ["python", "migration.py"]
