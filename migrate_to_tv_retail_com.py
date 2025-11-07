"""
Migrate existing data from walmart_tv_detail_crawled, amazon_tv_detail_crawled,
and bby_tv_detail_crawled to unified tv_retail_com table
"""
import psycopg2
import re
from config import DB_CONFIG
from datetime import datetime

def parse_star_ratings(star_ratings_str):
    """Parse star ratings string to get total count
    Examples:
    - "5star:142, 4star:14, 3star:7, 2star:2, 1star:4" -> 169
    - "5stars:231 4stars:19 3stars:2 2stars:1 1star:8" -> 261
    """
    if not star_ratings_str:
        return None
    try:
        numbers = re.findall(r':(\d+)', str(star_ratings_str))
        if numbers:
            return sum(int(n) for n in numbers)
    except:
        pass
    return None

def parse_count_of_reviews(count_str):
    """Parse count_of_reviews to integer
    Examples:
    - "1,123" -> 1123
    - "20" -> 20
    - 100 -> 100
    """
    if not count_str:
        return None
    try:
        if isinstance(count_str, int):
            return count_str
        return int(str(count_str).replace(',', ''))
    except:
        return None

def migrate_walmart_data(conn):
    """Migrate Walmart data to tv_retail_com"""
    cursor = conn.cursor()

    print("\n" + "="*80)
    print("MIGRATING WALMART DATA")
    print("="*80)

    # Get total count
    cursor.execute("SELECT COUNT(*) FROM walmart_tv_detail_crawled")
    total = cursor.fetchone()[0]
    print(f"[INFO] Total Walmart records: {total:,}")

    # Fetch all data
    cursor.execute("""
        SELECT
            page_type, product_url, retailer_sku_name, item, star_rating,
            number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts,
            sku_popularity, savings, discount_type, shipping_info,
            count_of_star_ratings, retailer_sku_name_similar, detailed_review_content,
            calendar_week, crawl_strdatetime,
            final_sku_price, original_sku_price, pick_up_availability,
            shipping_availability, delivery_availability, sku_status,
            retailer_membership_discounts, available_quantity_for_purchase,
            inventory_status, main_rank, bsr_rank, screen_size, count_of_reviews
        FROM walmart_tv_detail_crawled
        ORDER BY id
    """)

    rows = cursor.fetchall()
    inserted = 0
    errors = 0

    for idx, row in enumerate(rows, 1):
        try:
            (page_type, product_url, retailer_sku_name, item, star_rating,
             number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts,
             sku_popularity, savings, discount_type, shipping_info,
             count_of_star_ratings, retailer_sku_name_similar, detailed_review_content,
             calendar_week, crawl_strdatetime,
             final_sku_price, original_sku_price, pick_up_availability,
             shipping_availability, delivery_availability, sku_status,
             retailer_membership_discounts, available_quantity_for_purchase,
             inventory_status, main_rank, bsr_rank, screen_size, count_of_reviews) = row

            # Parse count fields
            count_of_reviews_int = parse_count_of_reviews(count_of_reviews)
            count_of_star_ratings_int = parse_star_ratings(count_of_star_ratings)

            cursor.execute("""
                INSERT INTO tv_retail_com
                (item, account_name, page_type, count_of_reviews, retailer_sku_name, product_url,
                 star_rating, count_of_star_ratings, screen_size, sku_popularity,
                 final_sku_price, original_sku_price, savings, discount_type, offer,
                 pick_up_availability, shipping_availability, delivery_availability, shipping_info,
                 available_quantity_for_purchase, inventory_status, sku_status, retailer_membership_discounts,
                 detailed_review_content, summarized_review_content, top_mentions, recommendation_intent,
                 main_rank, bsr_rank, rank_1, rank_2, promotion_rank, trend_rank,
                 number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts, retailer_sku_name_similar,
                 estimated_annual_electricity_use, promotion_type,
                 calendar_week, crawl_strdatetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                item, 'Walmart', page_type, count_of_reviews_int, retailer_sku_name, product_url,
                star_rating, count_of_star_ratings_int, screen_size, sku_popularity,
                final_sku_price, original_sku_price, savings, discount_type, None,  # offer
                pick_up_availability, shipping_availability, delivery_availability, shipping_info,
                available_quantity_for_purchase, inventory_status, sku_status, retailer_membership_discounts,
                detailed_review_content, None, None, None,  # summarized_review_content, top_mentions, recommendation_intent
                main_rank, bsr_rank, None, None, None, None,  # rank_1, rank_2, promotion_rank, trend_rank
                number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts, retailer_sku_name_similar,
                None, None,  # estimated_annual_electricity_use, promotion_type
                calendar_week, crawl_strdatetime
            ))
            inserted += 1

            if idx % 100 == 0:
                conn.commit()
                print(f"  [PROGRESS] {idx:,}/{total:,} ({idx/total*100:.1f}%)")

        except Exception as e:
            errors += 1
            if errors <= 5:  # Show first 5 errors
                print(f"  [ERROR] Row {idx}: {e}")

    conn.commit()
    cursor.close()

    print(f"[OK] Walmart migration complete: {inserted:,} inserted, {errors:,} errors")
    return inserted, errors

def migrate_amazon_data(conn):
    """Migrate Amazon data to tv_retail_com"""
    cursor = conn.cursor()

    print("\n" + "="*80)
    print("MIGRATING AMAZON DATA")
    print("="*80)

    # Get total count
    cursor.execute("SELECT COUNT(*) FROM amazon_tv_detail_crawled")
    total = cursor.fetchone()[0]
    print(f"[INFO] Total Amazon records: {total:,}")

    # Fetch all data
    cursor.execute("""
        SELECT
            page_type, product_url, retailer_sku_name, star_rating, sku_popularity,
            retailer_membership_discounts, item, rank_1, rank_2, count_of_star_ratings,
            summarized_review_content, detailed_review_content, calendar_week,
            crawl_strdatetime, screen_size, count_of_reviews, main_rank, bsr_rank
        FROM amazon_tv_detail_crawled
        ORDER BY id
    """)

    rows = cursor.fetchall()
    inserted = 0
    errors = 0

    for idx, row in enumerate(rows, 1):
        try:
            (page_type, product_url, retailer_sku_name, star_rating, sku_popularity,
             retailer_membership_discounts, item, rank_1, rank_2, count_of_star_ratings,
             summarized_review_content, detailed_review_content, calendar_week,
             crawl_strdatetime, screen_size, count_of_reviews, main_rank, bsr_rank) = row

            # Parse count fields
            count_of_reviews_int = parse_count_of_reviews(count_of_reviews)
            count_of_star_ratings_int = parse_star_ratings(count_of_star_ratings)

            cursor.execute("""
                INSERT INTO tv_retail_com
                (item, account_name, page_type, count_of_reviews, retailer_sku_name, product_url,
                 star_rating, count_of_star_ratings, screen_size, sku_popularity,
                 final_sku_price, original_sku_price, savings, discount_type, offer,
                 pick_up_availability, shipping_availability, delivery_availability, shipping_info,
                 available_quantity_for_purchase, inventory_status, sku_status, retailer_membership_discounts,
                 detailed_review_content, summarized_review_content, top_mentions, recommendation_intent,
                 main_rank, bsr_rank, rank_1, rank_2, promotion_rank, trend_rank,
                 number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts, retailer_sku_name_similar,
                 estimated_annual_electricity_use, promotion_type,
                 calendar_week, crawl_strdatetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                item, 'Amazon', page_type, count_of_reviews_int, retailer_sku_name, product_url,
                star_rating, count_of_star_ratings_int, screen_size, sku_popularity,
                None, None, None, None, None,  # final_sku_price, original_sku_price, savings, discount_type, offer
                None, None, None, None,  # pick_up_availability, shipping_availability, delivery_availability, shipping_info
                None, None, None, retailer_membership_discounts,  # available_quantity_for_purchase, inventory_status, sku_status, retailer_membership_discounts
                detailed_review_content, summarized_review_content, None, None,  # detailed_review_content, summarized_review_content, top_mentions, recommendation_intent
                main_rank, bsr_rank, rank_1, rank_2, None, None,  # main_rank, bsr_rank, rank_1, rank_2, promotion_rank, trend_rank
                None, None, None,  # number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts, retailer_sku_name_similar
                None, None,  # estimated_annual_electricity_use, promotion_type
                calendar_week, crawl_strdatetime
            ))
            inserted += 1

            if idx % 100 == 0:
                conn.commit()
                print(f"  [PROGRESS] {idx:,}/{total:,} ({idx/total*100:.1f}%)")

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  [ERROR] Row {idx}: {e}")

    conn.commit()
    cursor.close()

    print(f"[OK] Amazon migration complete: {inserted:,} inserted, {errors:,} errors")
    return inserted, errors

def migrate_bestbuy_data(conn):
    """Migrate BestBuy data to tv_retail_com"""
    cursor = conn.cursor()

    print("\n" + "="*80)
    print("MIGRATING BESTBUY DATA")
    print("="*80)

    # Get total count
    cursor.execute("SELECT COUNT(*) FROM bby_tv_detail_crawled")
    total = cursor.fetchone()[0]
    print(f"[INFO] Total BestBuy records: {total:,}")

    # Fetch all data
    cursor.execute("""
        SELECT
            page_type, retailer_sku_name, item, estimated_annual_electricity_use,
            screen_size, count_of_star_ratings, top_mentions, detailed_review_content,
            recommendation_intent, product_url, calendar_week, crawl_strdatetime,
            count_of_reviews, final_sku_price, savings, original_sku_price, offer,
            pick_up_availability, shipping_availability, delivery_availability, sku_status,
            star_rating, promotion_type, promotion_rank, bsr_rank, main_rank, trend_rank
        FROM bby_tv_detail_crawled
        ORDER BY id
    """)

    rows = cursor.fetchall()
    inserted = 0
    errors = 0

    for idx, row in enumerate(rows, 1):
        try:
            (page_type, retailer_sku_name, item, estimated_annual_electricity_use,
             screen_size, count_of_star_ratings, top_mentions, detailed_review_content,
             recommendation_intent, product_url, calendar_week, crawl_strdatetime,
             count_of_reviews, final_sku_price, savings, original_sku_price, offer,
             pick_up_availability, shipping_availability, delivery_availability, sku_status,
             star_rating, promotion_type, promotion_rank, bsr_rank, main_rank, trend_rank) = row

            # Parse count fields
            count_of_reviews_int = parse_count_of_reviews(count_of_reviews)
            count_of_star_ratings_int = parse_star_ratings(count_of_star_ratings)

            cursor.execute("""
                INSERT INTO tv_retail_com
                (item, account_name, page_type, count_of_reviews, retailer_sku_name, product_url,
                 star_rating, count_of_star_ratings, screen_size, sku_popularity,
                 final_sku_price, original_sku_price, savings, discount_type, offer,
                 pick_up_availability, shipping_availability, delivery_availability, shipping_info,
                 available_quantity_for_purchase, inventory_status, sku_status, retailer_membership_discounts,
                 detailed_review_content, summarized_review_content, top_mentions, recommendation_intent,
                 main_rank, bsr_rank, rank_1, rank_2, promotion_rank, trend_rank,
                 number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts, retailer_sku_name_similar,
                 estimated_annual_electricity_use, promotion_type,
                 calendar_week, crawl_strdatetime)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                item, 'Bestbuy', page_type, count_of_reviews_int, retailer_sku_name, product_url,
                star_rating, count_of_star_ratings_int, screen_size, None,  # sku_popularity
                final_sku_price, original_sku_price, savings, None, offer,  # discount_type
                pick_up_availability, shipping_availability, delivery_availability, None,  # shipping_info
                None, None, sku_status, None,  # available_quantity_for_purchase, inventory_status, sku_status, retailer_membership_discounts
                detailed_review_content, None, top_mentions, recommendation_intent,  # detailed_review_content, summarized_review_content, top_mentions, recommendation_intent
                main_rank, bsr_rank, None, None, promotion_rank, trend_rank,  # main_rank, bsr_rank, rank_1, rank_2, promotion_rank, trend_rank
                None, None, None,  # number_of_ppl_purchased_yesterday, number_of_ppl_added_to_carts, retailer_sku_name_similar
                estimated_annual_electricity_use, promotion_type,
                calendar_week, crawl_strdatetime
            ))
            inserted += 1

            if idx % 100 == 0:
                conn.commit()
                print(f"  [PROGRESS] {idx:,}/{total:,} ({idx/total*100:.1f}%)")

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"  [ERROR] Row {idx}: {e}")

    conn.commit()
    cursor.close()

    print(f"[OK] BestBuy migration complete: {inserted:,} inserted, {errors:,} errors")
    return inserted, errors

def main():
    """Main migration function"""
    print("\n" + "="*80)
    print("TV RETAIL COM - DATA MIGRATION")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        print("[OK] Database connected")

        # Check current tv_retail_com count
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tv_retail_com")
        current_count = cursor.fetchone()[0]
        cursor.close()
        print(f"[INFO] Current tv_retail_com records: {current_count:,}")

        if current_count > 0:
            response = input(f"\n⚠️  tv_retail_com already has {current_count:,} records. Continue? (yes/no): ")
            if response.lower() != 'yes':
                print("[INFO] Migration cancelled by user")
                return

        # Migrate data from each source
        total_inserted = 0
        total_errors = 0

        # Walmart
        inserted, errors = migrate_walmart_data(conn)
        total_inserted += inserted
        total_errors += errors

        # Amazon
        inserted, errors = migrate_amazon_data(conn)
        total_inserted += inserted
        total_errors += errors

        # BestBuy
        inserted, errors = migrate_bestbuy_data(conn)
        total_inserted += inserted
        total_errors += errors

        # Final summary
        print("\n" + "="*80)
        print("MIGRATION SUMMARY")
        print("="*80)
        print(f"Total inserted: {total_inserted:,}")
        print(f"Total errors: {total_errors:,}")

        # Check final count
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tv_retail_com")
        final_count = cursor.fetchone()[0]
        cursor.close()

        print(f"Final tv_retail_com records: {final_count:,}")
        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*80)

        conn.close()
        print("\n[SUCCESS] Migration completed!")

    except Exception as e:
        print(f"\n[ERROR] Migration failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
