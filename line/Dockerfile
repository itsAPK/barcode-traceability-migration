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

# docker run --rm \
#   -e TELEGRAM_BOT_TOKEN=xxx \
#   -e TELEGRAM_CHAT_ID=xxx \
#   -e MSSQL_SERVER="localhost\\SQLEXPRESS" \
#   -e MSSQL_DB="your_mssql_db" \
#   -e MYSQL_HOST="10.111.0.147" \
#   -e MYSQL_USER="root" \
#   -e MYSQL_PASSWORD="Arbl@123" \
#   -e MYSQL_DB="sslabel" \
#   -e MYSQL_PORT=3306 \
#   data-migrator
