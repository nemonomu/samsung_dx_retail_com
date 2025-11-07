"""
Create tv_retail_com unified table
"""
import psycopg2
from config import DB_CONFIG

def create_tv_retail_com_table():
    """Create unified TV retail data table"""
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("[INFO] Creating tv_retail_com table...")

        # Create table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tv_retail_com (
                id SERIAL PRIMARY KEY,

                -- 1. Fixed columns (requested order)
                item VARCHAR(255),
                account_name VARCHAR(255),
                page_type VARCHAR(50),
                count_of_reviews INTEGER,
                retailer_sku_name TEXT,
                product_url TEXT,

                -- 2. Basic product information
                star_rating VARCHAR(10),
                count_of_star_ratings INTEGER,
                screen_size VARCHAR(50),
                sku_popularity VARCHAR(100),

                -- 3. Price information
                final_sku_price VARCHAR(50),
                original_sku_price VARCHAR(50),
                savings VARCHAR(50),
                discount_type VARCHAR(100),
                offer TEXT,

                -- 4. Inventory/Shipping information
                pick_up_availability VARCHAR(50),
                shipping_availability VARCHAR(50),
                delivery_availability VARCHAR(50),
                shipping_info TEXT,
                available_quantity_for_purchase VARCHAR(50),
                inventory_status VARCHAR(100),
                sku_status VARCHAR(100),
                retailer_membership_discounts VARCHAR(100),

                -- 5. Reviews/Content
                detailed_review_content TEXT,
                summarized_review_content TEXT,       -- Amazon only
                top_mentions TEXT,                     -- BestBuy only
                recommendation_intent VARCHAR(100),    -- BestBuy only

                -- 6. Rankings
                main_rank INTEGER,
                bsr_rank INTEGER,
                rank_1 VARCHAR(255),                   -- Amazon only
                rank_2 VARCHAR(255),                   -- Amazon only
                promotion_rank INTEGER,                -- BestBuy only
                trend_rank INTEGER,                    -- BestBuy only

                -- 7. Retailer-specific columns
                number_of_ppl_purchased_yesterday INTEGER,  -- Walmart only
                number_of_ppl_added_to_carts INTEGER,       -- Walmart only
                retailer_sku_name_similar TEXT,             -- Walmart only
                estimated_annual_electricity_use VARCHAR(50), -- BestBuy only
                promotion_type VARCHAR(100),                -- BestBuy only

                -- 8. Metadata (last column)
                calendar_week VARCHAR(20),
                crawl_strdatetime TIMESTAMP
            )
        """)

        print("[OK] Table tv_retail_com created successfully")

        # Create indexes
        print("[INFO] Creating indexes...")

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tv_retail_com_item ON tv_retail_com(item)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tv_retail_com_account ON tv_retail_com(account_name)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tv_retail_com_page_type ON tv_retail_com(page_type)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tv_retail_com_crawl_time ON tv_retail_com(crawl_strdatetime)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_tv_retail_com_calendar_week ON tv_retail_com(calendar_week)
        """)

        print("[OK] Indexes created successfully")

        # Add table comment
        cursor.execute("""
            COMMENT ON TABLE tv_retail_com IS 'Unified table for TV retail data from Walmart, Amazon, and BestBuy'
        """)

        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()

        print("\n[SUCCESS] tv_retail_com table creation completed!")
        print("You can now view the table in DBeaver")

    except Exception as e:
        print(f"[ERROR] Failed to create table: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_tv_retail_com_table()
