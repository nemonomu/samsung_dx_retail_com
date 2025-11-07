"""
walmart_tv_detail_crawled 테이블 마이그레이션
1. mother → page_type 이름 변경
2. created_at → crawl_strdatetime 이름 변경 (형식: 202511051100000000)
3. sku → item 이름 변경
4. 11개 컬럼 추가:
   - final_sku_price, original_sku_price, pick_up_availability, shipping_availability,
   - delivery_availability, sku_status, retailer_membership_discounts,
   - available_quantity_for_purchase, inventory_status, main_rank, bsr_rank
"""
import psycopg2
from config import DB_CONFIG

def migrate_walmart_tv_detail_crawled():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("walmart_tv_detail_crawled 테이블 마이그레이션")
        print("="*80)

        # 1. mother → page_type 이름 변경
        print("\n[1] mother → page_type 컬럼 이름 변경 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'walmart_tv_detail_crawled'
            AND column_name = 'mother'
        """)

        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE walmart_tv_detail_crawled
                RENAME COLUMN mother TO page_type
            """)
            print("  [OK] mother → page_type 변경 완료")
        else:
            print("  [INFO] mother 컬럼이 없거나 이미 변경되었습니다")

        # 2. sku → item 이름 변경
        print("\n[2] sku → item 컬럼 이름 변경 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'walmart_tv_detail_crawled'
            AND column_name = 'sku'
        """)

        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE walmart_tv_detail_crawled
                RENAME COLUMN sku TO item
            """)
            print("  [OK] sku → item 변경 완료")
        else:
            print("  [INFO] sku 컬럼이 없거나 이미 변경되었습니다")

        # 3. created_at → crawl_strdatetime 이름 변경 및 형식 변환
        print("\n[3] created_at → crawl_strdatetime 변경 중...")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'walmart_tv_detail_crawled'
            AND column_name = 'created_at'
        """)

        if cursor.fetchone():
            # 임시 컬럼 생성 (VARCHAR)
            cursor.execute("""
                ALTER TABLE walmart_tv_detail_crawled
                ADD COLUMN crawl_strdatetime VARCHAR(50)
            """)
            print("  [OK] crawl_strdatetime 컬럼 생성")

            # created_at 데이터를 새 형식으로 변환하여 crawl_strdatetime에 복사
            cursor.execute("""
                UPDATE walmart_tv_detail_crawled
                SET crawl_strdatetime = TO_CHAR(created_at, 'YYYYMMDDHH24MISS') || '0000'
                WHERE created_at IS NOT NULL
            """)
            print("  [OK] 데이터 변환 완료 (형식: 202511051100000000)")

            # created_at 컬럼 삭제
            cursor.execute("""
                ALTER TABLE walmart_tv_detail_crawled
                DROP COLUMN created_at
            """)
            print("  [OK] created_at 컬럼 삭제")
        else:
            print("  [INFO] created_at 컬럼이 없거나 이미 변경되었습니다")

        # 4. 11개 컬럼 추가
        print("\n[4] 새 컬럼 추가 중...")
        columns_to_add = [
            ('final_sku_price', 'TEXT'),
            ('original_sku_price', 'TEXT'),
            ('pick_up_availability', 'TEXT'),
            ('shipping_availability', 'TEXT'),
            ('delivery_availability', 'TEXT'),
            ('sku_status', 'VARCHAR(50)'),
            ('retailer_membership_discounts', 'TEXT'),
            ('available_quantity_for_purchase', 'TEXT'),
            ('inventory_status', 'TEXT'),
            ('main_rank', 'INTEGER'),
            ('bsr_rank', 'INTEGER')
        ]

        for col_name, col_type in columns_to_add:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'walmart_tv_detail_crawled'
                AND column_name = %s
            """, (col_name,))

            if not cursor.fetchone():
                cursor.execute(f"""
                    ALTER TABLE walmart_tv_detail_crawled
                    ADD COLUMN {col_name} {col_type}
                """)
                print(f"  [OK] {col_name} 컬럼 추가 완료")
            else:
                print(f"  [INFO] {col_name} 컬럼이 이미 존재합니다")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] walmart_tv_detail_crawled 테이블 마이그레이션 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[확인] walmart_tv_detail_crawled 테이블 컬럼:")
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'walmart_tv_detail_crawled'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()
        for col_name, col_type in columns:
            print(f"  {col_name}: {col_type}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n[ERROR] 마이그레이션 실패: {e}")
        if conn:
            conn.rollback()
            conn.close()
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    migrate_walmart_tv_detail_crawled()
