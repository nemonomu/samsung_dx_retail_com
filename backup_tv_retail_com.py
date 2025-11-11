import psycopg2
from config import DB_CONFIG
from datetime import datetime

def backup_tv_retail_com():
    """Backup tv_retail_com table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Create backup table name with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_table = f'tv_retail_com_backup_{timestamp}'

        print(f"Creating backup table: {backup_table}")

        # Create backup table as a copy of tv_retail_com
        cursor.execute(f"""
            CREATE TABLE {backup_table} AS
            SELECT * FROM tv_retail_com
        """)

        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {backup_table}")
        count = cursor.fetchone()[0]

        conn.commit()

        print(f"[OK] Backup created: {backup_table}")
        print(f"[OK] Total rows backed up: {count:,}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] Backup failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    backup_tv_retail_com()
