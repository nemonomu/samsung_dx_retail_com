import psycopg2
from config import DB_CONFIG

def backfill_amazon_prices():
    """
    Backfill price information for amazon_tv_detail_crawled and tv_retail_com
    using latest batch_id data from amazon_tv_main_crawled and amazon_tv_bsr
    """
    try:
        print("="*80)
        print("Backfilling Amazon Price Information")
        print("="*80)

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Step 1: Get latest batch_ids for each table
        print("\n[STEP 1] Finding latest batch_ids for each table...")

        # amazon_tv_detail_crawled latest batch_id
        cursor.execute("""
            SELECT batch_id
            FROM amazon_tv_detail_crawled
            WHERE batch_id IS NOT NULL
            ORDER BY batch_id DESC
            LIMIT 1
        """)
        detail_batch = cursor.fetchone()
        detail_batch_id = detail_batch[0] if detail_batch else None
        print(f"  amazon_tv_detail_crawled latest batch_id: {detail_batch_id}")

        # tv_retail_com latest batch_id (Amazon only, using crawl_strdatetime)
        cursor.execute("""
            SELECT crawl_strdatetime
            FROM tv_retail_com
            WHERE account_name = 'Amazon'
              AND crawl_strdatetime IS NOT NULL
            ORDER BY crawl_strdatetime DESC
            LIMIT 1
        """)
        retail_batch = cursor.fetchone()
        retail_batch_id = retail_batch[0] if retail_batch else None
        print(f"  tv_retail_com latest batch_id: {retail_batch_id}")

        # amazon_tv_main_crawled latest batch_id
        cursor.execute("""
            SELECT batch_id
            FROM amazon_tv_main_crawled
            WHERE batch_id IS NOT NULL
            ORDER BY batch_id DESC
            LIMIT 1
        """)
        main_batch = cursor.fetchone()
        main_batch_id = main_batch[0] if main_batch else None
        print(f"  amazon_tv_main_crawled latest batch_id: {main_batch_id}")

        # amazon_tv_bsr latest batch_id
        cursor.execute("""
            SELECT batch_id
            FROM amazon_tv_bsr
            WHERE batch_id IS NOT NULL
            ORDER BY batch_id DESC
            LIMIT 1
        """)
        bsr_batch = cursor.fetchone()
        bsr_batch_id = bsr_batch[0] if bsr_batch else None
        print(f"  amazon_tv_bsr latest batch_id: {bsr_batch_id}")

        if not all([detail_batch_id, main_batch_id, bsr_batch_id]):
            print("\n[ERROR] Missing batch_ids. Cannot proceed.")
            return

        # Step 2: Update amazon_tv_detail_crawled
        print("\n[STEP 2] Updating amazon_tv_detail_crawled...")

        # Check how many rows need update
        cursor.execute("""
            SELECT COUNT(*)
            FROM amazon_tv_detail_crawled
            WHERE batch_id = %s
              AND (final_sku_price IS NULL OR original_sku_price IS NULL)
        """, (detail_batch_id,))
        detail_need_update = cursor.fetchone()[0]
        print(f"  Rows needing price update: {detail_need_update}")

        if detail_need_update > 0:
            # Update from Main (has both prices)
            cursor.execute("""
                UPDATE amazon_tv_detail_crawled AS d
                SET
                    final_sku_price = COALESCE(d.final_sku_price, m.final_sku_price),
                    original_sku_price = COALESCE(d.original_sku_price, m.original_sku_price)
                FROM amazon_tv_main_crawled AS m
                WHERE d.batch_id = %s
                  AND m.batch_id = %s
                  AND d.product_url = m.product_url
                  AND (d.final_sku_price IS NULL OR d.original_sku_price IS NULL)
            """, (detail_batch_id, main_batch_id))
            updated_from_main = cursor.rowcount
            print(f"  Updated from main: {updated_from_main} rows")

            # Update from BSR (only final_sku_price)
            cursor.execute("""
                UPDATE amazon_tv_detail_crawled AS d
                SET final_sku_price = COALESCE(d.final_sku_price, b.final_sku_price)
                FROM amazon_tv_bsr AS b
                WHERE d.batch_id = %s
                  AND b.batch_id = %s
                  AND d.product_url = b.product_url
                  AND d.final_sku_price IS NULL
            """, (detail_batch_id, bsr_batch_id))
            updated_from_bsr = cursor.rowcount
            print(f"  Updated from BSR: {updated_from_bsr} rows")

            conn.commit()
            print(f"  [OK] Total updated: {updated_from_main + updated_from_bsr} rows")
        else:
            print("  [INFO] No rows need update")

        # Step 3: Update tv_retail_com from amazon_tv_detail_crawled
        if retail_batch_id and detail_batch_id:
            print("\n[STEP 3] Updating tv_retail_com from amazon_tv_detail_crawled...")

            # Check how many rows need update
            cursor.execute("""
                SELECT COUNT(*)
                FROM tv_retail_com
                WHERE account_name = 'Amazon'
                  AND crawl_strdatetime = %s
                  AND (final_sku_price IS NULL OR original_sku_price IS NULL)
            """, (retail_batch_id,))
            retail_need_update = cursor.fetchone()[0]
            print(f"  Rows needing price update: {retail_need_update}")

            if retail_need_update > 0:
                # Update from amazon_tv_detail_crawled (already updated from main/bsr)
                cursor.execute("""
                    UPDATE tv_retail_com AS t
                    SET
                        final_sku_price = COALESCE(t.final_sku_price, d.final_sku_price),
                        original_sku_price = COALESCE(t.original_sku_price, d.original_sku_price)
                    FROM amazon_tv_detail_crawled AS d
                    WHERE t.account_name = 'Amazon'
                      AND t.crawl_strdatetime = %s
                      AND d.batch_id = %s
                      AND t.product_url = d.product_url
                      AND (t.final_sku_price IS NULL OR t.original_sku_price IS NULL)
                """, (retail_batch_id, detail_batch_id))
                retail_updated = cursor.rowcount
                print(f"  Updated from amazon_tv_detail_crawled: {retail_updated} rows")

                conn.commit()
                print(f"  [OK] Total updated: {retail_updated} rows")
            else:
                print("  [INFO] No rows need update")
        else:
            print("\n[STEP 3] Skipping tv_retail_com (no batch_id found)")

        # Step 4: Summary
        print("\n" + "="*80)
        print("Backfill Summary:")
        print("="*80)

        # Check remaining NULL prices in detail_crawled
        cursor.execute("""
            SELECT COUNT(*)
            FROM amazon_tv_detail_crawled
            WHERE batch_id = %s
              AND (final_sku_price IS NULL OR original_sku_price IS NULL)
        """, (detail_batch_id,))
        remaining_detail = cursor.fetchone()[0]
        print(f"amazon_tv_detail_crawled: {remaining_detail} rows still have NULL prices")

        # Check remaining NULL prices in tv_retail_com
        if retail_batch_id:
            cursor.execute("""
                SELECT COUNT(*)
                FROM tv_retail_com
                WHERE account_name = 'Amazon'
                  AND crawl_strdatetime = %s
                  AND (final_sku_price IS NULL OR original_sku_price IS NULL)
            """, (retail_batch_id,))
            remaining_retail = cursor.fetchone()[0]
            print(f"tv_retail_com: {remaining_retail} rows still have NULL prices")

        print("="*80)
        print("Backfill completed!")
        print("="*80)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n[FATAL ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    backfill_amazon_prices()
