"""
bby_tv_detail_crawled 테이블에 14개 컬럼 추가
- 9개 데이터 컬럼: final_sku_price, savings, original_sku_price, offer,
  pick_up_availability, shipping_availability, delivery_availability, sku_status, star_rating
- 5개 rank/type 컬럼: promotion_type, promotion_rank, bsr_rank, main_rank, trend_rank
"""
import psycopg2
from config import DB_CONFIG

def add_detail_columns():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("bby_tv_detail_crawled 테이블에 14개 컬럼 추가")
        print("="*80)

        # 추가할 컬럼 정의 (name, type)
        columns_to_add = [
            ('final_sku_price', 'VARCHAR(50)'),
            ('savings', 'VARCHAR(50)'),
            ('original_sku_price', 'VARCHAR(50)'),
            ('offer', 'VARCHAR(50)'),
            ('pick_up_availability', 'VARCHAR(50)'),
            ('shipping_availability', 'VARCHAR(50)'),
            ('delivery_availability', 'VARCHAR(50)'),
            ('sku_status', 'VARCHAR(50)'),
            ('star_rating', 'VARCHAR(50)'),
            ('promotion_type', 'TEXT'),
            ('promotion_rank', 'INTEGER'),
            ('bsr_rank', 'INTEGER'),
            ('main_rank', 'INTEGER'),
            ('trend_rank', 'INTEGER')
        ]

        for col_name, col_type in columns_to_add:
            print(f"\n[처리중] {col_name} 컬럼...")

            # 컬럼 존재 확인
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'bby_tv_detail_crawled'
                AND column_name = %s
            """, (col_name,))

            if cursor.fetchone():
                print(f"  [INFO] {col_name} 컬럼이 이미 존재합니다")
            else:
                cursor.execute(f"""
                    ALTER TABLE bby_tv_detail_crawled
                    ADD COLUMN {col_name} {col_type}
                """)
                print(f"  [OK] {col_name} 컬럼 추가 완료")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 모든 컬럼 추가 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[변경 결과 확인]")
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_detail_crawled'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()
        print("\nbby_tv_detail_crawled 테이블 컬럼:")
        for col_name, col_type in columns:
            print(f"  {col_name}: {col_type}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n[ERROR] 컬럼 추가 실패: {e}")
        if conn:
            conn.rollback()
            conn.close()
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    add_detail_columns()
