import psycopg2

# Import database configuration
from config import DB_CONFIG

def check_xpath():
    """Check product_name xpath in database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT data_field, xpath
            FROM xpath_selectors
            WHERE mall_name = 'Amazon' AND data_field = 'product_name'
        """)

        result = cursor.fetchone()
        print("="*80)
        print("Current product_name XPath:")
        print("="*80)
        if result:
            print(f"Data Field: {result[0]}")
            print(f"XPath: {result[1]}")
        else:
            print("[ERROR] product_name xpath not found in database")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_xpath()
    input("\nPress Enter to exit...")
