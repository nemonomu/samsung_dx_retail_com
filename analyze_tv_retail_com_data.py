"""
Analyze tv_retail_com table data before cleanup
Shows how many rows will be affected by each cleanup rule
"""
import psycopg2
from config import DB_CONFIG


def analyze_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print('=' * 80)
        print('TV_RETAIL_COM DATA ANALYSIS')
        print('=' * 80)
        print('Condition: crawl_datetime >= 2025-11-11 00:00:00')

        # Total rows
        cur.execute("""
            SELECT COUNT(*)
            FROM tv_retail_com
            WHERE crawl_datetime >= '2025-11-11 00:00:00'
        """)
        total = cur.fetchone()[0]
        print(f'Total rows to analyze: {total:,}\n')

        print('=' * 80)
        print('PRICE DATA CONSISTENCY ISSUES')
        print('=' * 80)

        # Case 1
        cur.execute("""
            SELECT COUNT(*)
            FROM tv_retail_com
            WHERE crawl_datetime >= '2025-11-11 00:00:00'
            AND final_sku_price IS NOT NULL
            AND savings IS NOT NULL
            AND original_sku_price IS NULL
        """)
        case1 = cur.fetchone()[0]
        print(f'Case 1: fsp + savings + no osp -> delete savings')
        print(f'  Affected rows: {case1:,}
')

        # Case 2
        cur.execute("""
            SELECT COUNT(*)
            FROM tv_retail_com
            WHERE crawl_datetime >= '2025-11-11 00:00:00'
            AND final_sku_price IS NOT NULL
            AND savings IS NULL
            AND original_sku_price IS NOT NULL
        """)
        case2 = cur.fetchone()[0]
        print(f'Case 2: fsp + no savings + osp -> delete osp')
        print(f'  Affected rows: {case2:,}
')

        # Case 3
        cur.execute("""
            SELECT COUNT(*)
            FROM tv_retail_com
            WHERE crawl_datetime >= '2025-11-11 00:00:00'
            AND final_sku_price IS NULL
            AND (savings IS NOT NULL OR original_sku_price IS NOT NULL)
        """)
        case3 = cur.fetchone()[0]
        print(f'Case 3: no fsp + (savings or osp) -> delete both')
        print(f'  Affected rows: {case3:,}
')

        print('=' * 80)
        print('PRICE FORMAT CLEANING')
        print('=' * 80)

        # fsp with extra text
        cur.execute("""
            SELECT COUNT(*)
            FROM tv_retail_com
            WHERE crawl_datetime >= '2025-11-11 00:00:00'
            AND final_sku_price LIKE '%with%'
        """)
        fsp_with_text = cur.fetchone()[0]
        print(f'fsp with extra text: {fsp_with_text:,}')

        if fsp_with_text > 0:
            cur.execute("""
                SELECT final_sku_price
                FROM tv_retail_com
                WHERE crawl_datetime >= '2025-11-11 00:00:00'
                AND final_sku_price LIKE '%with%'
                LIMIT 3
            """)
            print('  Samples:')
            for row in cur.fetchall():
                print(f'    - {row[0]}')
        print()

        print('=' * 80)
        print('SCREEN_SIZE FORMAT')
        print('=' * 80)

        # screen_size without inches
        cur.execute("""
            SELECT COUNT(*)
            FROM tv_retail_com
            WHERE crawl_datetime >= '2025-11-11 00:00:00'
            AND screen_size IS NOT NULL
            AND screen_size NOT LIKE '%inches%'
        """)
        size_no_inches = cur.fetchone()[0]
        print(f'screen_size without inches: {size_no_inches:,}')

        if size_no_inches > 0:
            cur.execute("""
                SELECT DISTINCT screen_size
                FROM tv_retail_com
                WHERE crawl_datetime >= '2025-11-11 00:00:00'
                AND screen_size IS NOT NULL
                AND screen_size NOT LIKE '%inches%'
                LIMIT 10
            """)
            print('  Samples:')
            for row in cur.fetchall():
                print(f'    - {row[0]}')
        print()

        print('=' * 80)
        print('SUMMARY')
        print('=' * 80)
        print(f'Total rows: {total:,}')
        print(f'Price consistency issues: {case1 + case2 + case3:,}')
        print(f'  - Case 1 (delete savings): {case1:,}')
        print(f'  - Case 2 (delete osp): {case2:,}')
        print(f'  - Case 3 (delete both): {case3:,}')
        print(f'Price format cleaning: {fsp_with_text:,}')
        print(f'Screen size format: {size_no_inches:,}')
        print('=' * 80)

        cur.close()
        conn.close()

    except Exception as e:
        print(f'ERROR: {e}')
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    analyze_data()
