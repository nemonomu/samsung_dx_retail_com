import psycopg2
from config import DB_CONFIG
from datetime import datetime

class TVRetailPriceBackfill:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.stats = {
            'Amazon': {'updated': 0, 'failed': 0},
            'Bestbuy': {'updated': 0, 'failed': 0},
            'Walmart': {'updated': 0, 'failed': 0}
        }

    def connect_db(self):
        """Connect to database"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.conn.autocommit = False  # Manual commit for safety
            self.cursor = self.conn.cursor()
            print("[OK] Database connected")
            return True
        except Exception as e:
            print(f"[ERROR] Database connection failed: {e}")
            return False

    def get_null_price_rows(self, account_name, date_range=None):
        """Get rows with NULL prices from tv_retail_com"""
        try:
            query = """
                SELECT id, product_url, crawl_strdatetime
                FROM tv_retail_com
                WHERE account_name = %s
                  AND (final_sku_price IS NULL OR original_sku_price IS NULL)
            """
            params = [account_name]

            if date_range:
                query += " AND crawl_strdatetime BETWEEN %s AND %s"
                params.extend(date_range)

            query += " ORDER BY crawl_strdatetime"

            self.cursor.execute(query, params)
            return self.cursor.fetchall()
        except Exception as e:
            print(f"[ERROR] Failed to get NULL price rows: {e}")
            return []

    def process_amazon(self, priority_date_range=None):
        """Process Amazon price backfill"""
        print("\n" + "="*80)
        print("Processing Amazon")
        print("="*80)

        # Get NULL price rows
        if priority_date_range:
            print(f"[Priority] Processing date range: {priority_date_range[0]} ~ {priority_date_range[1]}")
            null_rows = self.get_null_price_rows('Amazon', priority_date_range)
        else:
            print("[INFO] Processing all NULL prices")
            null_rows = self.get_null_price_rows('Amazon')

        print(f"[INFO] Found {len(null_rows)} rows with NULL prices")

        if not null_rows:
            print("[INFO] No rows to process")
            return

        updated_count = 0

        for idx, (row_id, product_url, crawl_strdatetime) in enumerate(null_rows, 1):
            try:
                if idx % 10 == 0:
                    print(f"[Progress] {idx}/{len(null_rows)} rows processed...")

                # Step 1: Get batch_id from amazon_tv_detail_crawled
                self.cursor.execute("""
                    SELECT batch_id
                    FROM amazon_tv_detail_crawled
                    WHERE crawl_strdatetime = %s
                    LIMIT 1
                """, (crawl_strdatetime,))

                detail_result = self.cursor.fetchone()
                if not detail_result:
                    continue

                detail_batch_id = detail_result[0]

                # Step 2: Find most recent batch_ids before detail_batch_id
                # Main
                self.cursor.execute("""
                    SELECT batch_id
                    FROM amazon_tv_main_crawled
                    WHERE batch_id < %s
                    ORDER BY batch_id DESC
                    LIMIT 1
                """, (detail_batch_id,))
                main_batch = self.cursor.fetchone()
                main_batch_id = main_batch[0] if main_batch else None

                # BSR
                self.cursor.execute("""
                    SELECT batch_id
                    FROM amazon_tv_bsr
                    WHERE batch_id < %s
                    ORDER BY batch_id DESC
                    LIMIT 1
                """, (detail_batch_id,))
                bsr_batch = self.cursor.fetchone()
                bsr_batch_id = bsr_batch[0] if bsr_batch else None

                # Step 3: Get prices from main (has both final and original)
                final_price = None
                original_price = None

                if main_batch_id:
                    self.cursor.execute("""
                        SELECT final_sku_price, original_sku_price
                        FROM amazon_tv_main_crawled
                        WHERE batch_id = %s
                          AND product_url = %s
                        LIMIT 1
                    """, (main_batch_id, product_url))
                    main_result = self.cursor.fetchone()
                    if main_result:
                        final_price, original_price = main_result

                # Step 4: If still NULL, get final_price from BSR (no original_sku_price in BSR)
                if not final_price and bsr_batch_id:
                    self.cursor.execute("""
                        SELECT final_sku_price
                        FROM amazon_tv_bsr
                        WHERE batch_id = %s
                          AND product_url = %s
                        LIMIT 1
                    """, (bsr_batch_id, product_url))
                    bsr_result = self.cursor.fetchone()
                    if bsr_result:
                        final_price = bsr_result[0]

                # Step 5: Update tv_retail_com
                if final_price or original_price:
                    self.cursor.execute("""
                        UPDATE tv_retail_com
                        SET final_sku_price = COALESCE(final_sku_price, %s),
                            original_sku_price = COALESCE(original_sku_price, %s)
                        WHERE id = %s
                    """, (final_price, original_price, row_id))

                    if self.cursor.rowcount > 0:
                        updated_count += 1

            except Exception as e:
                print(f"[ERROR] Row {idx} failed: {e}")
                self.stats['Amazon']['failed'] += 1
                continue

        self.conn.commit()
        self.stats['Amazon']['updated'] = updated_count
        print(f"[OK] Amazon: {updated_count} rows updated")

    def process_bestbuy(self, priority_date_range=None):
        """Process Best Buy price backfill"""
        print("\n" + "="*80)
        print("Processing Best Buy")
        print("="*80)

        # Get NULL price rows
        if priority_date_range:
            print(f"[Priority] Processing date range: {priority_date_range[0]} ~ {priority_date_range[1]}")
            null_rows = self.get_null_price_rows('Bestbuy', priority_date_range)
        else:
            print("[INFO] Processing all NULL prices")
            null_rows = self.get_null_price_rows('Bestbuy')

        print(f"[INFO] Found {len(null_rows)} rows with NULL prices")

        if not null_rows:
            print("[INFO] No rows to process")
            return

        updated_count = 0

        for idx, (row_id, product_url, crawl_strdatetime) in enumerate(null_rows, 1):
            try:
                if idx % 10 == 0:
                    print(f"[Progress] {idx}/{len(null_rows)} rows processed...")

                # Step 1: Get batch_id from bby_tv_detail_crawled
                self.cursor.execute("""
                    SELECT batch_id
                    FROM bby_tv_detail_crawled
                    WHERE crawl_strdatetime = %s
                    LIMIT 1
                """, (crawl_strdatetime,))

                detail_result = self.cursor.fetchone()
                if not detail_result:
                    continue

                detail_batch_id = detail_result[0]

                # Step 2: Find most recent batch_ids before detail_batch_id
                # Main
                self.cursor.execute("""
                    SELECT batch_id
                    FROM bestbuy_tv_main_crawl
                    WHERE batch_id < %s
                    ORDER BY batch_id DESC
                    LIMIT 1
                """, (detail_batch_id,))
                main_batch = self.cursor.fetchone()
                main_batch_id = main_batch[0] if main_batch else None

                # BSR
                self.cursor.execute("""
                    SELECT batch_id
                    FROM bby_tv_bsr_crawl
                    WHERE batch_id < %s
                    ORDER BY batch_id DESC
                    LIMIT 1
                """, (detail_batch_id,))
                bsr_batch = self.cursor.fetchone()
                bsr_batch_id = bsr_batch[0] if bsr_batch else None

                # Promotion
                self.cursor.execute("""
                    SELECT batch_id
                    FROM bby_tv_promotion_crawl
                    WHERE batch_id < %s
                    ORDER BY batch_id DESC
                    LIMIT 1
                """, (detail_batch_id,))
                promo_batch = self.cursor.fetchone()
                promo_batch_id = promo_batch[0] if promo_batch else None

                # Step 3: Get prices (priority: Main > BSR > Promotion)
                final_price = None
                original_price = None

                # Try Main
                if main_batch_id:
                    self.cursor.execute("""
                        SELECT final_sku_price, original_sku_price
                        FROM bestbuy_tv_main_crawl
                        WHERE batch_id = %s
                          AND product_url = %s
                        LIMIT 1
                    """, (main_batch_id, product_url))
                    main_result = self.cursor.fetchone()
                    if main_result:
                        final_price, original_price = main_result

                # Try BSR if still NULL
                if (not final_price or not original_price) and bsr_batch_id:
                    self.cursor.execute("""
                        SELECT final_sku_price, original_sku_price
                        FROM bby_tv_bsr_crawl
                        WHERE batch_id = %s
                          AND product_url = %s
                        LIMIT 1
                    """, (bsr_batch_id, product_url))
                    bsr_result = self.cursor.fetchone()
                    if bsr_result:
                        if not final_price:
                            final_price = bsr_result[0]
                        if not original_price:
                            original_price = bsr_result[1]

                # Try Promotion if still NULL
                if (not final_price or not original_price) and promo_batch_id:
                    self.cursor.execute("""
                        SELECT final_sku_price, original_sku_price
                        FROM bby_tv_promotion_crawl
                        WHERE batch_id = %s
                          AND product_url = %s
                        LIMIT 1
                    """, (promo_batch_id, product_url))
                    promo_result = self.cursor.fetchone()
                    if promo_result:
                        if not final_price:
                            final_price = promo_result[0]
                        if not original_price:
                            original_price = promo_result[1]

                # Step 4: Update tv_retail_com
                if final_price or original_price:
                    self.cursor.execute("""
                        UPDATE tv_retail_com
                        SET final_sku_price = COALESCE(final_sku_price, %s),
                            original_sku_price = COALESCE(original_sku_price, %s)
                        WHERE id = %s
                    """, (final_price, original_price, row_id))

                    if self.cursor.rowcount > 0:
                        updated_count += 1

            except Exception as e:
                print(f"[ERROR] Row {idx} failed: {e}")
                self.stats['Bestbuy']['failed'] += 1
                continue

        self.conn.commit()
        self.stats['Bestbuy']['updated'] = updated_count
        print(f"[OK] Best Buy: {updated_count} rows updated")

    def process_walmart(self, priority_date_range=None):
        """Process Walmart price backfill"""
        print("\n" + "="*80)
        print("Processing Walmart")
        print("="*80)

        # Get NULL price rows
        if priority_date_range:
            print(f"[Priority] Processing date range: {priority_date_range[0]} ~ {priority_date_range[1]}")
            null_rows = self.get_null_price_rows('Walmart', priority_date_range)
        else:
            print("[INFO] Processing all NULL prices")
            null_rows = self.get_null_price_rows('Walmart')

        print(f"[INFO] Found {len(null_rows)} rows with NULL prices")

        if not null_rows:
            print("[INFO] No rows to process")
            return

        updated_count = 0

        for idx, (row_id, product_url, crawl_strdatetime) in enumerate(null_rows, 1):
            try:
                if idx % 10 == 0:
                    print(f"[Progress] {idx}/{len(null_rows)} rows processed...")

                # Step 1: walmart_tv_detail_crawled does NOT have batch_id
                # Find most recent batch_ids before crawl_strdatetime directly from main/bsr tables

                # Find Main batch_id
                self.cursor.execute("""
                    SELECT batch_id
                    FROM wmart_tv_main_crawl
                    WHERE crawl_strdatetime < %s
                    ORDER BY crawl_strdatetime DESC
                    LIMIT 1
                """, (crawl_strdatetime,))
                main_batch = self.cursor.fetchone()
                main_batch_id = main_batch[0] if main_batch else None

                # Find BSR batch_id
                self.cursor.execute("""
                    SELECT batch_id
                    FROM wmart_tv_bsr_crawl
                    WHERE crawl_strdatetime < %s
                    ORDER BY crawl_strdatetime DESC
                    LIMIT 1
                """, (crawl_strdatetime,))
                bsr_batch = self.cursor.fetchone()
                bsr_batch_id = bsr_batch[0] if bsr_batch else None

                # Step 3: Get prices (priority: Main > BSR)
                final_price = None
                original_price = None

                # Try Main
                if main_batch_id:
                    self.cursor.execute("""
                        SELECT final_sku_price, original_sku_price
                        FROM wmart_tv_main_crawl
                        WHERE batch_id = %s
                          AND product_url = %s
                        LIMIT 1
                    """, (main_batch_id, product_url))
                    main_result = self.cursor.fetchone()
                    if main_result:
                        final_price, original_price = main_result

                # Try BSR if still NULL
                if (not final_price or not original_price) and bsr_batch_id:
                    self.cursor.execute("""
                        SELECT final_sku_price, original_sku_price
                        FROM wmart_tv_bsr_crawl
                        WHERE batch_id = %s
                          AND product_url = %s
                        LIMIT 1
                    """, (bsr_batch_id, product_url))
                    bsr_result = self.cursor.fetchone()
                    if bsr_result:
                        if not final_price:
                            final_price = bsr_result[0]
                        if not original_price:
                            original_price = bsr_result[1]

                # Step 4: Update tv_retail_com
                if final_price or original_price:
                    self.cursor.execute("""
                        UPDATE tv_retail_com
                        SET final_sku_price = COALESCE(final_sku_price, %s),
                            original_sku_price = COALESCE(original_sku_price, %s)
                        WHERE id = %s
                    """, (final_price, original_price, row_id))

                    if self.cursor.rowcount > 0:
                        updated_count += 1

            except Exception as e:
                print(f"[ERROR] Row {idx} failed: {e}")
                self.stats['Walmart']['failed'] += 1
                continue

        self.conn.commit()
        self.stats['Walmart']['updated'] = updated_count
        print(f"[OK] Walmart: {updated_count} rows updated")

    def run(self):
        """Main execution"""
        try:
            print("="*80)
            print("TV Retail Price Backfill")
            print("="*80)

            if not self.connect_db():
                return

            # Priority date range: 20251027 ~ 20251105
            priority_range = ('20251027000000', '20251105235959')

            print("\n[PHASE 1] Processing priority date range: 20251027 ~ 20251105")
            print("="*80)

            self.process_amazon(priority_range)
            self.process_bestbuy(priority_range)
            self.process_walmart(priority_range)

            print("\n[PHASE 2] Processing remaining NULL prices")
            print("="*80)

            self.process_amazon(None)
            self.process_bestbuy(None)
            self.process_walmart(None)

            # Summary
            print("\n" + "="*80)
            print("BACKFILL SUMMARY")
            print("="*80)
            for account, stats in self.stats.items():
                print(f"{account}:")
                print(f"  Updated: {stats['updated']}")
                print(f"  Failed: {stats['failed']}")

            print("="*80)
            print("Backfill completed!")
            print("="*80)

        except Exception as e:
            print(f"\n[FATAL ERROR] {e}")
            import traceback
            traceback.print_exc()
            if self.conn:
                self.conn.rollback()

        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()

if __name__ == "__main__":
    backfill = TVRetailPriceBackfill()
    backfill.run()
