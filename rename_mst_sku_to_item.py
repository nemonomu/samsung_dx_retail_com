"""
bby_tv_mst 테이블의 sku 컬럼을 item으로 변경
"""
import psycopg2
from config import DB_CONFIG

def rename_sku_to_item():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("bby_tv_mst 테이블 sku -> item 컬럼명 변경 시작")
        print("="*80)

        # sku 컬럼을 item으로 변경
        print("\n[INFO] sku 컬럼을 item으로 변경 중...")

        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_mst'
            AND column_name = 'sku'
        """)

        if cursor.fetchone():
            cursor.execute("""
                ALTER TABLE bby_tv_mst
                RENAME COLUMN sku TO item
            """)
            print("  [OK] sku -> item 변경 완료")
        else:
            print("  [INFO] sku 컬럼이 이미 item으로 변경되었거나 존재하지 않습니다")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 컬럼명 변경 완료!")
        print("="*80)

        # 변경 결과 확인
        print("\n[변경 결과 확인]")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'bby_tv_mst'
            ORDER BY ordinal_position
        """)

        columns = [row[0] for row in cursor.fetchall()]
        print(f"\nbby_tv_mst 테이블 컬럼:")
        print(f"  {', '.join(columns)}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n[ERROR] 마이그레이션 실패: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise

if __name__ == "__main__":
    rename_sku_to_item()
