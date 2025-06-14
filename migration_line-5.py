import pyodbc
import pymysql
from datetime import datetime
import logging
import requests
import os
from typing import Dict, Any, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging with file output
log_file = 'migration.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# Telegram Configuration
TELEGRAM_BOT_TOKEN = '1393190801:AAFSRCGOQAajiyY7SE5kxTDTcaPDecOQAjs'
TELEGRAM_CHAT_ID = '431108047'

# Database Configurations
mssql_config = {
    'driver': '{SQL Server}',
    'server': 'localhost\\SQLEXPRESS',
    'database': 'AmaraRaja_SBD_Finishing',
    'trusted_connection': 'yes'
}

mysql_config = {
    'host': '10.111.0.147',
    'user': 'root',
    'password': 'Arbl@123',
    'database': 'sslabel',
    'port': 3306,
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
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message,
            'parse_mode': 'HTML'
        }
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logging.info('Telegram notification sent successfully')
    except Exception as e:
        logging.error(f'Failed to send Telegram notification: {str(e)}')
        raise

def validate_row_data(row: Dict[str, Any]) -> Tuple[bool, str]:
    if not row:
        return False, "Empty row data"
    required_fields = ['Datetime', 'LineNo', 'Shift', 'ProductID', 'ScannedCode',"Synced"]
    for field in required_fields:
        if field not in row or row[field] is None:
            return False, f"Missing or null required field: {field}"
    return True, ""

def get_week_and_year(date_time: datetime) -> Tuple[int, int]:
    iso_calendar = date_time.isocalendar()
    return iso_calendar[1], iso_calendar[0]

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
    last_entry_time = None  # Initialize to avoid reference before assignment
    stats = {
        'records_processed': 0,
        'records_inserted': 0,
        'records_failed': 0,
        'validation_failures': 0
    }

    try:
        logging.info("Starting data migration process")

        with pyodbc.connect(**mssql_config) as mssql_conn, \
             pymysql.connect(**mysql_config) as mysql_conn:
            
            mssql_cursor = mssql_conn.cursor()
            mysql_cursor = mysql_conn.cursor()

            last_entry = execute_with_retry(mysql_cursor, "SELECT MAX(entrytime) AS last_entry_time FROM barcode where lineno = 5")
            if last_entry and last_entry[0]['last_entry_time']:
                last_entry_time = last_entry[0]['last_entry_time']
                mssql_query = """
                SELECT 
                    [ID],
                    [LineNo],
                    [Shift],
                    [Mode],
                    CAST([Datetime] AS DATETIME) AS Datetime,
                    [ProductID],
                    [ScannedCode],
                    [PrintedCode],
                    [VerifiedCode],
                    [Synced]
                FROM [ScanLogs]
                WHERE CAST([Datetime] AS DATETIME) > ?
                ORDER BY [Datetime] ASC
                """
                params = (last_entry_time,)
            else:
                mssql_query = """
                SELECT TOP 25000
                    [ID],
                    [LineNo],
                    [Shift],
                    [Mode],
                    CAST([Datetime] AS DATETIME) AS Datetime,
                    [ProductID],
                    [ScannedCode],
                    [PrintedCode],
                    [VerifiedCode],
                    [Synced]
                FROM [ScanLogs]
                ORDER BY [Datetime] DESC
                """
                params = None

                logging.info("No previous entries found. Fetching last 5000 records.")

            if params:
                mssql_cursor.execute(mssql_query, params)
            else:
                mssql_cursor.execute(mssql_query)

            rows = [dict(zip([column[0] for column in mssql_cursor.description], row)) for row in mssql_cursor.fetchall()]
            if not last_entry or not last_entry[0]['last_entry_time']:
                rows = list(reversed(rows))

            total_rows = len(rows)
            logging.info(f"Found {total_rows} new records to process")
            
            product_master = mssql_cursor.execute("SELECT * FROM ProductMaster").fetchall()

            product_id_map = {
    row.ID: (row.PlantCode.strip(), row.ProductCode.strip())
    for row in product_master
}

            batch_size = 100
            for i in range(0, total_rows, batch_size):
                batch = rows[i:i + batch_size]
                batch_values = []

                for row in batch:
                    stats['records_processed'] += 1

                    is_valid, error_message = validate_row_data(row)
                    if not is_valid:
                        stats['validation_failures'] += 1
                        logging.warning(f"Validation failed for record {stats['records_processed']}: {error_message}")
                        continue

                    try:
                       date_time = row['Datetime']
                       week, year = get_week_and_year(date_time)
                       plant_code, product_code = product_id_map.get(row['ProductID'], (None, None))

                       batch_values.append((
                            row['LineNo'],
                            row['PrintedCode'],
                            row['ScannedCode'],
                            row['PrintedCode'][-5:] if row['PrintedCode'] else None,
                            date_time,
                            row['Shift'],
                            week,
                            year,
                            row['Synced'],
                            datetime.now(),
                            datetime.now(),
                            f"{plant_code}{product_code}"
))

                    except Exception as e:
                        stats['records_failed'] += 1
                        logging.error(f"Error processing record {stats['records_processed']}: {str(e)}")

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
                        logging.info(f"Inserted batch of {len(batch_values)} records. Progress: {stats['records_processed']}/{total_rows}")
                    except Exception as e:
                        mysql_conn.rollback()
                        stats['records_failed'] += len(batch_values)
                        logging.error(f"Batch insertion failed: {str(e)}")

            execution_time = datetime.now() - start_time
            notification_message = f"🔄 <b>SBD1 Barcode Traceability Data Migration Report</b>\n\n" \
                                   f"📍 Line: 5\n" \
                                   f"⏰ Run Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n" \
                                   f"📊 Statistics:\n" \
                                   f"  • Records Processed: {stats['records_processed']}\n" \
                                   f"  • Successfully Inserted: {stats['records_inserted']}\n" \
                                   f"  • Failed: {stats['records_failed']}\n" \
                                   f"  • Validation Failures: {stats['validation_failures']}\n" \
                                   f"⌛ Last Entry Time: {last_entry_time if last_entry_time else 'Initial Load'}\n" \
                                   f"⏱ Execution Duration: {execution_time}\n" \
                                   f"✅ Status: Success"

            send_telegram_notification(notification_message)

    except Exception as e:
        error_message = f"Migration failed: {str(e)}"
        logging.error(error_message)

        notification_message = f"🔄 <b>SBD1 Barcode Traceability Data Migration Report</b>\n\n" \
                               f"📍 Line: 5\n" \
                               f"⏰ Run Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n" \
                               f"❌ Status: Failed\n" \
                               f"📝 Error: {str(e)}\n" \
                               f"📊 Partial Statistics:\n" \
                               f"  • Records Processed: {stats['records_processed']}\n" \
                               f"  • Successfully Inserted: {stats['records_inserted']}\n" \
                               f"  • Failed: {stats['records_failed']}\n" \
                               f"  • Validation Failures: {stats['validation_failures']}"

        send_telegram_notification(notification_message)
        raise

if __name__ == "__main__":
    migrate_data()