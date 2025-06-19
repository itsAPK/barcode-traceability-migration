import pyodbc
import pymysql
from datetime import datetime
import logging
import requests
from typing import Dict, Any, Tuple, List
from tenacity import retry, stop_after_attempt, wait_exponential
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

# Telegram config
TELEGRAM_BOT_TOKEN = '1393190801:AAFSRCGOQAajiyY7SE5kxTDTcaPDecOQAjs'
TELEGRAM_CHAT_ID = '431108047'
# MSSQL
mssql_config = {
    'driver': '{SQL Server}',
    'server': 'localhost\\SQLEXPRESS',
    'database': 'AmaraRaja_SBD_Line1_Finishing',
    'trusted_connection': 'yes'
}

# MySQL
mysql_config = {
    'host': '10.111.0.147',
    'user': 'root',
    'password': 'Arbl@123',
    'database': 'sslabel',
    'port': 3306,
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': False,
    'connect_timeout': 60,
    'read_timeout': 120,
    'write_timeout': 120,
}

# Thread-safe stats
stats_lock = Lock()
stats = {
    'records_processed': 0,
    'records_inserted': 0,
    'records_failed': 0,
    'validation_failures': 0
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=4, max=10))
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
        logging.info('Telegram sent')
    except Exception as e:
        logging.error(f'Telegram error: {e}')

def validate_row_data(row: Dict[str, Any]) -> Tuple[bool, str]:
    required = ['Date_Time', 'Line', 'Finishing_Code', 'Formation_Code', 'Count', 'Shift', 'Battery_Model']
    for field in required:
        if not row.get(field):
            return False, f"Missing: {field}"
    return True, ""

def get_week_and_year(date_time: datetime) -> Tuple[int, int]:
    iso = date_time.isocalendar()
    return iso[1], iso[0]

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=4, max=10))
def get_mysql_connection():
    return pymysql.connect(**mysql_config)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=4, max=10))
def insert_batch(cursor, conn, values: List[tuple]):
    try:
        cursor.executemany("""
            INSERT INTO barcode (
                lineno, barcode, battery_serial_no, serial_no, entrytime, shift,
                week, year, status, created_at, updated_at, productcode
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, values)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"Batch insert failed: {e}")
        raise

def process_batch(batch: List[Dict[str, Any]]):
    local_stats = {
        'processed': 0,
        'inserted': 0,
        'failed': 0,
        'validation_failures': 0
    }
    values = []

    try:
        for row in batch:
            local_stats['processed'] += 1
            valid, msg = validate_row_data(row)
            if not valid:
                local_stats['validation_failures'] += 1
                logging.warning(f"Validation failed: {msg}")
                continue

            try:
                dt = row['Date_Time']
                week, year = get_week_and_year(dt)
                values.append((
                    row['Line'],
                    row['Formation_Code'],
                    row['Finishing_Code'],
                    row['Finishing_Code'][-5:] if row['Finishing_Code'] else None,
                    dt,
                    row['Shift'],
                    week,
                    year,
                    1,
                    datetime.now(),
                    datetime.now(),
                    f"B{row['Battery_Model']}"
                ))
            except Exception as e:
                local_stats['failed'] += 1
                logging.error(f"Parse error: {e}")

        if values:
            conn = get_mysql_connection()
            cursor = conn.cursor()
            insert_batch(cursor, conn, values)
            local_stats['inserted'] += len(values)
            cursor.close()
            conn.close()
            logging.info(f"✅ Inserted {len(values)} records")
    except Exception as e:
        local_stats['failed'] += len(values)
        logging.error(f"Thread error: {e}")

    # Update shared stats
    with stats_lock:
        stats['records_processed'] += local_stats['processed']
        stats['records_inserted'] += local_stats['inserted']
        stats['records_failed'] += local_stats['failed']
        stats['validation_failures'] += local_stats['validation_failures']

def migrate_data():
    start = datetime.now()
    last_entry_time = None

    try:
        logging.info("🚀 Starting threaded migration")

        with pyodbc.connect(**mssql_config) as mssql_conn:
            mssql_cursor = mssql_conn.cursor()
            mysql_conn = get_mysql_connection()
            mysql_cursor = mysql_conn.cursor()

            result = mysql_cursor.execute("SELECT MAX(entrytime) AS last_entry_time FROM barcode WHERE lineno = 1")
            last = mysql_cursor.fetchone()
            last_entry_time = last['last_entry_time'] if last and last['last_entry_time'] else None

            if last_entry_time:
                query = """
                    SELECT CASE WHEN ISDATE([Date_Time]) = 1 THEN CAST([Date_Time] AS DATETIME) ELSE NULL END AS Date_Time,
                           [Shift], [Line], [Count], [Mode], [Battery_Model], [Battery_Type],
                           [FG_code], [Formation_Code], [Finishing_Code]
                    FROM [Battery_Printed_Logs]
                    WHERE ISDATE([Date_Time]) = 1 AND CAST([Date_Time] AS DATETIME) > ?
                    ORDER BY [Date_Time] ASC
                """
                mssql_cursor.execute(query, (last_entry_time,))
            else:
                logging.info("No previous entry, pulling latest 25000 records")
                query = """
                    SELECT TOP 25000
                        CASE WHEN ISDATE([Date_Time]) = 1 THEN CAST([Date_Time] AS DATETIME) ELSE NULL END AS Date_Time,
                        [Shift], [Line], [Count], [Mode], [Battery_Model], [Battery_Type],
                        [FG_code], [Formation_Code], [Finishing_Code]
                    FROM [Battery_Printed_Logs]
                    WHERE ISDATE([Date_Time]) = 1
                    ORDER BY [Date_Time] DESC
                """
                mssql_cursor.execute(query)

            rows = [dict(zip([col[0] for col in mssql_cursor.description], row)) for row in mssql_cursor.fetchall()]
            if not last_entry_time:
                rows.reverse()

            logging.info(f"📦 Fetched {len(rows)} rows")

            # Threaded processing
            batch_size = 100
            max_threads = 5
            batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                futures = [executor.submit(process_batch, batch) for batch in batches]
                for f in as_completed(futures):
                    pass  # Could log individual result/failure here

            execution_time = datetime.now() - start
            send_telegram_notification(
                f"✅ <b>SBD1 Threaded Migration Complete</b>\n\n"
                f"📍 Line: 1\n"
                f"⏰ Start: {start.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"📊 Stats:\n"
                f"• Processed: {stats['records_processed']}\n"
                f"• Inserted: {stats['records_inserted']}\n"
                f"• Failed: {stats['records_failed']}\n"
                f"• Validation Failures: {stats['validation_failures']}\n"
                f"⏱ Duration: {execution_time}"
            )

    except Exception as e:
        logging.error(f"🚨 Migration error: {e}")
        send_telegram_notification(
            f"❌ <b>SBD1 Threaded Migration Failed</b>\n\n"
            f"Error: {e}\n"
            f"⏱ Duration: {datetime.now() - start}"
        )

if __name__ == "__main__":
    migrate_data()
