import psycopg2

# Import database configuration
from config import DB_CONFIG

def create_ununique_tables():
    """Create new tables without UNIQUE constraints for collecting duplicates"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*80)
        print("Creating UNUNIQUE tables (no duplicate checking)")
        print("="*80)

        # Drop existing ununique tables if they exist
        print("\n[1/3] Dropping existing ununique tables if they exist...")
        cursor.execute("DROP TABLE IF EXISTS raw_data_ununique CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS Amazon_tv_main_crawled_ununique CASCADE;")
        print("[OK] Existing tables dropped")

        # Create raw_data_ununique table - NO UNIQUE constraint
        print("\n[2/3] Creating raw_data_ununique table...")
        cursor.execute("""
            CREATE TABLE raw_data_ununique (
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
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("[OK] raw_data_ununique table created")

        # Create Amazon_tv_main_crawled_ununique table - NO UNIQUE constraint
        print("\n[3/3] Creating Amazon_tv_main_crawled_ununique table...")
        cursor.execute("""
            CREATE TABLE Amazon_tv_main_crawled_ununique (
                id SERIAL PRIMARY KEY,
                mall_name VARCHAR(50) NOT NULL,
                Retailer_SKU_Name TEXT,
                Number_of_units_purchased_past_month VARCHAR(100),
                Final_SKU_Price VARCHAR(50),
                Original_SKU_Price VARCHAR(50),
                Shipping_Info TEXT,
                Available_Quantity_for_Purchase VARCHAR(100),
                Discount_Type VARCHAR(100),
                collected_at_local_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("[OK] Amazon_tv_main_crawled_ununique table created")

        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("SUCCESS: All ununique tables created!")
        print("="*80)
        print("\nTables created:")
        print("  1. raw_data_ununique - stores all data including duplicates")
        print("  2. Amazon_tv_main_crawled_ununique - stores main fields including duplicates")
        print("\nNote: NO UNIQUE constraints - duplicates will be saved!")

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_ununique_tables()
    print("\n[INFO] Script completed. Exiting...")
