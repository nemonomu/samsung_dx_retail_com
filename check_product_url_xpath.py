import psycopg2

# Import database configuration
from config import DB_CONFIG

def check_xpath():
    """Check product_url xpath in database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT data_field, xpath, css_selector
            FROM xpath_selectors
            WHERE mall_name = 'Amazon' AND data_field = 'product_url'
        """)

        result = cursor.fetchone()
        if result:
            print("="*80)
            print("Current product_url XPath:")
            print("="*80)
            print(f"Data Field: {result[0]}")
            print(f"XPath: {result[1]}")
            print(f"CSS: {result[2]}")
        else:
            print("[ERROR] product_url xpath not found in database")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_xpath()
    input("\nPress Enter to exit...")
