import psycopg2

# Import database configuration
from config import DB_CONFIG

def update_tables():
    """Rename collected_data to raw_data and create Amazon_tv_main_crawled table"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Updating Table Structure")
        print("="*80)

        # Drop old tables if exist
        print("\n[1] Dropping old tables...")
        cursor.execute("DROP TABLE IF EXISTS raw_data;")
        cursor.execute("DROP TABLE IF EXISTS Amazon_tv_main_crawled;")
        cursor.execute("DROP TABLE IF EXISTS collected_data;")

        # Create raw_data table (기존 collected_data와 동일)
        print("[2] Creating raw_data table...")
        cursor.execute("""
            CREATE TABLE raw_data (
                id SERIAL PRIMARY KEY,
                mall_name VARCHAR(50) NOT NULL,
                page_number INTEGER,
                Retailer_SKU_Name TEXT,
                Number_of_units_purchased_past_month VARCHAR(100),
                Final_SKU_Price VARCHAR(50),
                Original_SKU_Price VARCHAR(50),
                Shipping_Info TEXT,
                Available_Quantity_for_Purchase VARCHAR(100),
                Discount_Type VARCHAR(100),
                Product_URL TEXT,
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(mall_name, Retailer_SKU_Name)
            );
        """)

        # Create Amazon_tv_main_crawled table
        print("[3] Creating Amazon_tv_main_crawled table...")
        cursor.execute("""
            CREATE TABLE Amazon_tv_main_crawled (
                id SERIAL PRIMARY KEY,
                mall_name VARCHAR(50) NOT NULL,
                Retailer_SKU_Name TEXT,
                Number_of_units_purchased_past_month VARCHAR(100),
                Final_SKU_Price VARCHAR(50),
                Original_SKU_Price VARCHAR(50),
                Shipping_Info TEXT,
                Available_Quantity_for_Purchase VARCHAR(100),
                Discount_Type VARCHAR(100),
                collected_at_local_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(mall_name, Retailer_SKU_Name)
            );
        """)

        conn.commit()

        # Verify raw_data table
        print("\n" + "="*80)
        print("raw_data Table Structure:")
        print("="*80)
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'raw_data'
            ORDER BY ordinal_position;
        """)
        for row in cursor.fetchall():
            print(f"  {row[0]:<50} {row[1]}")

        # Verify Amazon_tv_main_crawled table
        print("\n" + "="*80)
        print("Amazon_tv_main_crawled Table Structure:")
        print("="*80)
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_main_crawled'
            ORDER BY ordinal_position;
        """)
        for row in cursor.fetchall():
            print(f"  {row[0]:<50} {row[1]}")

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("Tables created successfully!")
        print("="*80)

        return True

    except Exception as e:
        print(f"[ERROR] Failed to update tables: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("="*80)
    print("Renaming collected_data to raw_data")
    print("Creating Amazon_tv_main_crawled table")
    print("="*80)
    print()

    if update_tables():
        print("\n" + "="*80)
        print("Table update completed!")
        print("="*80)

    input("\nPress Enter to exit...")
