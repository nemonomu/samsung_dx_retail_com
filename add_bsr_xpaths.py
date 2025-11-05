"""
xpath_selectors 테이블에 BSR 페이지용 XPath 추가
- final_sku_price
- count_of_reviews
- star_rating
"""
import psycopg2
from config import DB_CONFIG

def add_bsr_xpaths():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("xpath_selectors 테이블에 BSR XPath 추가")
        print("="*80)

        # 추가할 XPath 목록
        xpaths_to_add = [
            ('final_sku_price', './/span[@class="a-size-base a-color-price"]//span[contains(@class, "_cDEzb_p13n-sc-price")]', None),
            ('count_of_reviews', './/span[@aria-hidden="true" and contains(@class, "a-size-small")]', None),
            ('star_rating', './/i[contains(@class, "a-icon-star-small")]//span[@class="a-icon-alt"]', None)
        ]

        for data_field, xpath, css_selector in xpaths_to_add:
            # 이미 존재하는지 확인
            cursor.execute("""
                SELECT id FROM xpath_selectors
                WHERE mall_name = 'Amazon'
                AND page_type = 'bsr_page'
                AND data_field = %s
            """, (data_field,))

            if cursor.fetchone():
                print(f"  [INFO] {data_field} XPath가 이미 존재합니다")
            else:
                cursor.execute("""
                    INSERT INTO xpath_selectors
                    (mall_name, page_type, data_field, xpath, css_selector, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, ('Amazon', 'bsr_page', data_field, xpath, css_selector, True))
                print(f"  [OK] {data_field} XPath 추가 완료")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] XPath 추가 완료!")
        print("="*80)

        # 결과 확인
        print("\n[확인] Amazon BSR 페이지 XPath 목록:")
        cursor.execute("""
            SELECT data_field, xpath
            FROM xpath_selectors
            WHERE mall_name = 'Amazon' AND page_type = 'bsr_page' AND is_active = TRUE
            ORDER BY data_field
        """)

        xpaths = cursor.fetchall()
        for field, xpath in xpaths:
            print(f"  {field}: {xpath[:80]}...")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n[ERROR] XPath 추가 실패: {e}")
        if conn:
            conn.rollback()
            conn.close()
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    add_bsr_xpaths()
