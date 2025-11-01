import psycopg2

# Import database configuration
from config import DB_CONFIG

def fix_xpath():
    """Fix product_url xpath in database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Update product_url xpath - a 태그가 h2 태그를 감싸고 있음
        new_xpath = './/a[.//h2]/@href'

        print("="*80)
        print("Fixing product_url XPath")
        print("="*80)
        print(f"New XPath: {new_xpath}")

        cursor.execute("""
            UPDATE xpath_selectors
            SET xpath = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE mall_name = 'Amazon'
            AND data_field = 'product_url'
            AND page_type = 'main_page'
        """, (new_xpath,))

        conn.commit()

        # Verify
        cursor.execute("""
            SELECT data_field, xpath
            FROM xpath_selectors
            WHERE mall_name = 'Amazon' AND data_field = 'product_url'
        """)

        result = cursor.fetchone()
        print(f"\n[OK] Updated!")
        print(f"Data Field: {result[0]}")
        print(f"XPath: {result[1]}")

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("XPath updated successfully!")
        print("="*80)

        return True

    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    fix_xpath()
    input("\nPress Enter to exit...")
