import pyodbc
import pymysql
from datetime import datetime
import logging
import requests
from typing import Dict, Any, Tuple
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
log_file = 'missing_serial.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Telegram Config
TELEGRAM_BOT_TOKEN = 'YOUR_BOT_TOKEN'
TELEGRAM_CHAT_ID = 'YOUR_CHAT_ID'

# Database Config
mssql_config = {
    'driver': '{SQL Server}',
    'server': 'localhost\\SQLEXPRESS',
    'database': 'AmaraRaja_SBD_Line1_Finishing',
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
    required_fields = ['Date_Time', 'Line', 'Finishing_Code', 'Formation_Code', 'Count', 'Shift', 'Battery_Model']
    for field in required_fields:
        if field not in row or row[field] is None:
            return False, f"Missing or null required field: {field}"
    return True, ""

def get_week_and_year(date_time: datetime) -> Tuple[int, int]:
    iso_calendar = date_time.isocalendar()
    return iso_calendar[1], iso_calendar[0]

def fetch_existing_serials(mysql_cursor) -> set:
    try:
        mysql_cursor.execute("SELECT battery_serial_no FROM barcode WHERE lineno = 1")
        rows = mysql_cursor.fetchall()
        return {row['battery_serial_no'] for row in rows}
    except Exception as e:
        logging.error(f"Failed to fetch existing serial numbers: {str(e)}")
        raise

def migrate_missing_serials() -> None:
    start_time = datetime.now()
    stats = {
        'records_processed': 0,
        'records_inserted': 0,
        'records_skipped': 0,
        'records_failed': 0,
        'validation_failures': 0
    }

    try:
        logging.info("Starting missing serial migration...")

        with pyodbc.connect(**mssql_config) as mssql_conn, \
             pymysql.connect(**mysql_config) as mysql_conn:

            mssql_cursor = mssql_conn.cursor()
            mysql_cursor = mysql_conn.cursor()

            # Fetch already inserted serials
            existing_serials = fetch_existing_serials(mysql_cursor)

            # Fetch all MSSQL records
            mssql_cursor.execute("""
                SELECT 
                    CASE WHEN ISDATE([Date_Time]) = 1 THEN CAST([Date_Time] AS DATETIME) ELSE NULL END AS Date_Time,
                    [Shift], [Line], [Count], [Mode], [Battery_Model], [Battery_Type],
                    [FG_code], [Formation_Code], [Finishing_Code]
                FROM [Battery_Printed_Logs]
                WHERE ISDATE([Date_Time]) = 1
                ORDER BY [Date_Time] ASC
            """)

            rows = [dict(zip([col[0] for col in mssql_cursor.description], row)) for row in mssql_cursor.fetchall()]
            total_rows = len(rows)
            logging.info(f"Fetched {total_rows} records from MSSQL")

            batch_size = 100
            for i in range(0, total_rows, batch_size):
                batch = rows[i:i + batch_size]
                batch_values = []

                for row in batch:
                    stats['records_processed'] += 1

                    is_valid, error_msg = validate_row_data(row)
                    if not is_valid:
                        stats['validation_failures'] += 1
                        logging.warning(f"Validation failed at row {stats['records_processed']}: {error_msg}")
                        continue

                    serial = row['Finishing_Code']
                    if not serial or serial in existing_serials:
                        stats['records_skipped'] += 1
                        continue

                    try:
                        date_time = row['Date_Time']
                        week, year = get_week_and_year(date_time)

                        batch_values.append((
                            row['Line'],
                            serial,
                            row['Formation_Code'],
                            serial[-5:] if serial else None,
                            date_time,
                            row['Shift'],
                            week,
                            year,
                            1,
                            datetime.now(),
                            datetime.now(),
                            f"B{row['Battery_Model']}"
                        ))
                    except Exception as e:
                        stats['records_failed'] += 1
                        logging.error(f"Processing error: {e}")

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
                        logging.info(f"Inserted batch of {len(batch_values)} records.")
                    except Exception as e:
                        mysql_conn.rollback()
                        stats['records_failed'] += len(batch_values)
                        logging.error(f"Batch insert failed: {str(e)}")

            duration = datetime.now() - start_time
            message = (
                f"🧾 <b>Missing Barcode Serial Migration Report</b>\n\n"
                f"📍 Line: 1\n"
                f"⏰ Start: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"📊 Stats:\n"
                f"  • Total MSSQL Records Scanned: {total_rows}\n"
                f"  • Processed: {stats['records_processed']}\n"
                f"  • Inserted: {stats['records_inserted']}\n"
                f"  • Skipped: {stats['records_skipped']}\n"
                f"  • Validation Errors: {stats['validation_failures']}\n"
                f"  • Failed Inserts: {stats['records_failed']}\n"
                f"⏱ Duration: {duration}"
            )
            send_telegram_notification(message)

    except Exception as e:
        logging.error(f"Migration failed: {e}")
        error_msg = (
            f"🚨 <b>Migration Failed</b>\n\n"
            f"❌ Error: {e}\n"
            f"📊 Stats:\n"
            f"  • Processed: {stats['records_processed']}\n"
            f"  • Inserted: {stats['records_inserted']}\n"
            f"  • Skipped: {stats['records_skipped']}\n"
            f"  • Validation Errors: {stats['validation_failures']}\n"
            f"  • Failed Inserts: {stats['records_failed']}"
        )
        send_telegram_notification(error_msg)
        raise

if __name__ == "__main__":
    migrate_missing_serials()
