"""
Walmart detail_page xpath 4개 키 존재 여부 확인 (일회성)
"""
import psycopg2
from config import DB_CONFIG

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("""
    SELECT data_field, xpath, is_active, updated_at
    FROM xpath_selectors
    WHERE mall_name = 'Walmart'
      AND page_type = 'detail_page'
      AND data_field IN ('discount_type', 'savings', 'original_price', 'offer')
    ORDER BY data_field
""")
rows = cur.fetchall()
print(f"Found {len(rows)} rows:")
for r in rows:
    print(f"  {r[0]:20s} | active={r[2]} | updated={r[3]}")
    print(f"    xpath: {r[1]}")
cur.close()
conn.close()
