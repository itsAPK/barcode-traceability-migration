import pyodbc
import pymysql
from datetime import datetime
import logging
import requests
import os
from typing import Dict, Any, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
load_dotenv()

log_file = 'migration.log'

from logging.handlers import RotatingFileHandler

file_handler = RotatingFileHandler(
    log_file, maxBytes=5 * 1024 * 1024, backupCount=0, encoding='utf-8'
)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, stream_handler]
)

# Telegram Configuration
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

LINENO = os.environ.get('LINENO')

# Database Configurations
mssql_config = {
    'driver': '{ODBC Driver 17 for SQL Server}',
    'server': os.environ.get('MSSQL_SERVER'),
    'database': os.environ.get('MSSQL_DB'),
    'trusted_connection': 'yes'
}

mysql_config = {
    'host': os.environ.get('MYSQL_HOST'),
    'user': os.environ.get('MYSQL_USER'),
    'password': os.environ.get('MYSQL_PASSWORD'),
    'database': os.environ.get('MYSQL_DB'),
    'port': int(os.environ.get('MYSQL_PORT', 3306)),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': False,
    'connect_timeout': 30,
    'read_timeout': 30,
    'write_timeout': 30,
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def send_telegram_notification(message: str) -> None:
    try:
        url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
        payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'HTML'}
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logging.info('Telegram notification sent successfully')
    except Exception as e:
        logging.error(f'Failed to send Telegram notification: {str(e)}')
        raise

def validate_row_data(row: Dict[str, Any]) -> Tuple[bool, str]:
    required_fields = ['Date_Time', 'Line', 'Finishing_Code', 'Formation_Code', 'Count', 'Shift', 'Battery_Model']
    for field in required_fields:
        if field not in row or row[field] is None:
            return False, f"Missing or null required field: {field}"
    return True, ""

def get_week_and_year(date_time: datetime) -> Tuple[int, int]:
    return date_time.isocalendar()[1], date_time.isocalendar()[0]

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def execute_with_retry(cursor, query: str, params: tuple = None) -> Any:
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor.fetchall() if query.strip().upper().startswith('SELECT') else None
    except Exception as e:
        logging.error(f"Database operation failed: {str(e)}")
        raise

def migrate_data() -> None:
    start_time = datetime.now()
    stats = {'records_processed': 0, 'records_inserted': 0, 'records_failed': 0, 'validation_failures': 0}

    try:
        logging.info("Starting data migration process")

        with pyodbc.connect(**mssql_config) as mssql_conn, \
             pymysql.connect(**mysql_config) as mysql_conn:

            mssql_cursor = mssql_conn.cursor()
            mysql_cursor = mysql_conn.cursor()

            last_entry = execute_with_retry(mysql_cursor, "SELECT MAX(entrytime) AS last_entry_time FROM barcode where lineno = 1")
            last_entry_time = last_entry[0]['last_entry_time'] if last_entry and last_entry[0]['last_entry_time'] else None

            if last_entry_time:
                mssql_query = """
                SELECT 
                    CASE WHEN ISDATE([Date_Time]) = 1 THEN CAST([Date_Time] AS DATETIME) ELSE NULL END AS Date_Time,
                    [Shift], [Line], [Count], [Mode], [Battery_Model], [Battery_Type],
                    [FG_code], [Formation_Code], [Finishing_Code]
                FROM [Battery_Printed_Logs]
                WHERE ISDATE([Date_Time]) = 1 AND CAST([Date_Time] AS DATETIME) > ?
                ORDER BY [Date_Time] ASC
                """
                mssql_cursor.execute(mssql_query, (last_entry_time,))
            else:
                mssql_query = """
                SELECT TOP 25000
                    CASE WHEN ISDATE([Date_Time]) = 1 THEN CAST([Date_Time] AS DATETIME) ELSE NULL END AS Date_Time,
                    [Shift], [Line], [Count], [Mode], [Battery_Model], [Battery_Type],
                    [FG_code], [Formation_Code], [Finishing_Code]
                FROM [Battery_Printed_Logs]
                WHERE ISDATE([Date_Time]) = 1
                ORDER BY [Date_Time] DESC
                """
                mssql_cursor.execute(mssql_query)

            rows = [dict(zip([column[0] for column in mssql_cursor.description], row)) for row in mssql_cursor.fetchall()]
            if not last_entry_time:
                rows = list(reversed(rows))

            logging.info(f"Found {len(rows)} new records to process")

            for i in range(0, len(rows), 100):
                batch = rows[i:i + 100]
                batch_values = []

                for row in batch:
                    stats['records_processed'] += 1
                    valid, error = validate_row_data(row)
                    if not valid:
                        stats['validation_failures'] += 1
                        continue

                    try:
                        week, year = get_week_and_year(row['Date_Time'])
                        batch_values.append((
                            row['Line'], row['Finishing_Code'], row['Formation_Code'],
                            row['Finishing_Code'][-5:], row['Date_Time'], row['Shift'], week,
                            year, 1, datetime.now(), datetime.now(), f"B{row['Battery_Model']}"
                        ))
                    except Exception:
                        stats['records_failed'] += 1

                if batch_values:
                    try:
                        mysql_cursor.executemany("""
                            INSERT INTO barcode (
                                lineno, barcode, battery_serial_no, serial_no, entrytime, shift,
                                week, year, status, created_at, updated_at, productcode
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, batch_values)
                        mysql_conn.commit()
                        stats['records_inserted'] += len(batch_values)
                    except Exception:
                        mysql_conn.rollback()
                        stats['records_failed'] += len(batch_values)

            execution_time = datetime.now() - start_time
            notification_message = f"🔄 <b>SBD1 Barcode Traceability Data Migration Report</b>\n\n" \
                                   f"📍 Line: {LINENO}\n" \
                                   f"⏰ Run Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n" \
                                   f"📊 Statistics:\n" \
                                   f"  • Records Processed: {stats['records_processed']}\n" \
                                   f"  • Successfully Inserted: {stats['records_inserted']}\n" \
                                   f"  • Failed: {stats['records_failed']}\n" \
                                   f"  • Validation Failures: {stats['validation_failures']}\n" \
                                   f"⌛ Last Entry Time: {last_entry_time if last_entry_time else 'Initial Load'}\n" \
                                   f"⏱ Execution Duration: {execution_time}\n" \
                                   f"✅ Status: Success"
            send_telegram_notification(
                notification_message   )

    except Exception as e:
        logging.error(f"Migration failed: {e}")
        notification_message = f"🔄 <b>SBD1 Barcode Traceability Data Migration Report</b>\n\n" \
                               f"📍 Line: {LINENO}\n" \
                               f"⏰ Run Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n" \
                               f"❌ Status: Failed\n" \
                               f"📝 Error: {str(e)}\n" \
                               f"📊 Partial Statistics:\n" \
                               f"  • Records Processed: {stats['records_processed']}\n" \
                               f"  • Successfully Inserted: {stats['records_inserted']}\n" \
                               f"  • Failed: {stats['records_failed']}\n" \
                               f"  • Validation Failures: {stats['validation_failures']}"

        send_telegram_notification(notification_message)

if __name__ == "__main__":
    migrate_data()
