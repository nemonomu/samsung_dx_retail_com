import psycopg2

# Import database configuration
from config import DB_CONFIG

def update_table_structure():
    """Update collected_data table to match required column names"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Drop old table
        print("[INFO] Dropping old table...")
        cursor.execute("DROP TABLE IF EXISTS collected_data;")

        # Create new table with correct column names
        print("[INFO] Creating new table with correct columns...")
        cursor.execute("""
            CREATE TABLE collected_data (
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

        conn.commit()
        print("[OK] Table structure updated successfully!")

        # Verify
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'collected_data'
            ORDER BY ordinal_position;
        """)

        print("\n" + "="*80)
        print("Updated Table Structure:")
        print("="*80)
        for row in cursor.fetchall():
            print(f"  {row[0]:<40} {row[1]}")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"[ERROR] Failed to update table: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("="*80)
    print("Updating collected_data Table Structure")
    print("="*80)
    print()

    if update_table_structure():
        print("\n" + "="*80)
        print("Table update completed!")
        print("="*80)

    input("\nPress Enter to exit...")
