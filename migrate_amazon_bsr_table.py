"""
amazon_tv_bsr 테이블 수정
1. 5개 컬럼 삭제: original_sku_price, number_of_units_purchased_past_month,
   shipping_info, available_quantity_for_purchase, discount_type
2. 4개 컬럼 추가: account_name, final_sku_price, count_of_reviews, star_rating
"""
import psycopg2
from config import DB_CONFIG

def migrate_amazon_bsr():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("amazon_tv_bsr 테이블 마이그레이션")
        print("="*80)

        # 삭제할 컬럼들
        columns_to_drop = [
            'original_sku_price',
            'number_of_units_purchased_past_month',
            'shipping_info',
            'available_quantity_for_purchase',
            'discount_type'
        ]

        print("\n[1] 불필요한 컬럼 삭제 중...")
        for col_name in columns_to_drop:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'amazon_tv_bsr'
                AND column_name = %s
            """, (col_name,))

            if cursor.fetchone():
                cursor.execute(f"""
                    ALTER TABLE amazon_tv_bsr
                    DROP COLUMN {col_name}
                """)
                print(f"  [OK] {col_name} 컬럼 삭제 완료")
            else:
                print(f"  [INFO] {col_name} 컬럼이 이미 삭제되었거나 존재하지 않습니다")

        # 추가할 컬럼들
        columns_to_add = [
            ('account_name', 'VARCHAR(50)'),
            ('final_sku_price', 'TEXT'),
            ('count_of_reviews', 'TEXT'),
            ('star_rating', 'VARCHAR(50)')
        ]

        print("\n[2] 새 컬럼 추가 중...")
        for col_name, col_type in columns_to_add:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'amazon_tv_bsr'
                AND column_name = %s
            """, (col_name,))

            if not cursor.fetchone():
                cursor.execute(f"""
                    ALTER TABLE amazon_tv_bsr
                    ADD COLUMN {col_name} {col_type}
                """)
                print(f"  [OK] {col_name} 컬럼 추가 완료")
            else:
                print(f"  [INFO] {col_name} 컬럼이 이미 존재합니다")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 테이블 마이그레이션 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[변경 결과 확인]")
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'amazon_tv_bsr'
            ORDER BY ordinal_position
        """)

        columns = cursor.fetchall()
        print("\namazon_tv_bsr 테이블 컬럼:")
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
    migrate_amazon_bsr()
