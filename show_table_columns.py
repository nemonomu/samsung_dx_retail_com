import psycopg2
from config import DB_CONFIG

def show_table_columns():
    """Show columns for all TV crawling tables"""

    tables = [
        # BestBuy tables
        'bestbuy_tv_main_crawl',
        'bby_tv_promotion_crawl',
        'bby_tv_trend_crawl',
        'bby_tv_detail_crawled',
        'bby_tv_mst',

        # Amazon tables
        'amazon_tv_main_crawled',
        'amazon_tv_bsr',
        'amazon_tv_detail_crawled',

        # Walmart tables
        'wmart_tv_main_crawl',
        'wmart_tv_bsr_crawl',
        'walmart_tv_detail_crawled',

        # Event table
        'bfd_event_crawl'
    ]

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("="*100)
        print("TV CRAWLING TABLES - COLUMN INFORMATION")
        print("="*100)

        for table_name in tables:
            # Check if table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                );
            """, (table_name,))

            exists = cursor.fetchone()[0]

            if not exists:
                print(f"\n[WARNING] Table '{table_name}' does not exist - SKIPPED")
                continue

            # Get column information
            cursor.execute("""
                SELECT
                    column_name,
                    data_type,
                    character_maximum_length,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position;
            """, (table_name,))

            columns = cursor.fetchall()

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            row_count = cursor.fetchone()[0]

            print(f"\n{'='*100}")
            print(f"TABLE: {table_name.upper()} (Rows: {row_count:,})")
            print(f"{'='*100}")
            print(f"{'#':<4} {'Column Name':<35} {'Type':<25} {'Nullable':<10} {'Default':<15}")
            print("-"*100)

            for idx, col in enumerate(columns, 1):
                col_name, data_type, max_length, is_nullable, default = col

                # Format data type
                if max_length:
                    type_str = f"{data_type}({max_length})"
                else:
                    type_str = data_type

                # Format default
                default_str = str(default)[:15] if default else '-'

                # Format nullable
                nullable_str = 'YES' if is_nullable == 'YES' else 'NO'

                print(f"{idx:<4} {col_name:<35} {type_str:<25} {nullable_str:<10} {default_str:<15}")

        cursor.close()
        conn.close()

        print("\n" + "="*100)
        print("SUMMARY COMPLETED")
        print("="*100)

    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    show_table_columns()
