import psycopg2
from config import DB_CONFIG

def add_columns():
    """Add price columns to amazon tables"""
    try:
        print("="*80)
        print("Adding price columns to Amazon tables")
        print("="*80)

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # 1. Add original_sku_price to amazon_tv_bsr
        print("\n[STEP 1/2] Adding original_sku_price to amazon_tv_bsr...")
        try:
            cursor.execute("""
                ALTER TABLE amazon_tv_bsr
                ADD COLUMN IF NOT EXISTS original_sku_price VARCHAR(50)
            """)
            conn.commit()
            print("[OK] original_sku_price added to amazon_tv_bsr")
        except Exception as e:
            print(f"[ERROR] Failed to add column to amazon_tv_bsr: {e}")
            conn.rollback()

        # 2. Add final_sku_price and original_sku_price to amazon_tv_detail_crawled
        print("\n[STEP 2/2] Adding price columns to amazon_tv_detail_crawled...")
        try:
            cursor.execute("""
                ALTER TABLE amazon_tv_detail_crawled
                ADD COLUMN IF NOT EXISTS final_sku_price VARCHAR(50),
                ADD COLUMN IF NOT EXISTS original_sku_price VARCHAR(50)
            """)
            conn.commit()
            print("[OK] final_sku_price and original_sku_price added to amazon_tv_detail_crawled")
        except Exception as e:
            print(f"[ERROR] Failed to add columns to amazon_tv_detail_crawled: {e}")
            conn.rollback()

        cursor.close()
        conn.close()

        print("\n" + "="*80)
        print("Column addition completed!")
        print("="*80)

    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    add_columns()
