"""
TV Crawling Monitoring and Alert Module
- Crawling result analysis
- Email alert when issues detected
"""

import smtplib
import csv
import io
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import pytz
import logging
import pandas as pd

from config import EMAIL_CONFIG

logger = logging.getLogger(__name__)

# Country/Retailer name mapping
RETAILER_NAMES = {
    'amazon': 'Amazon USA',
    'walmart': 'Walmart USA',
    'bestbuy': 'Best Buy USA'
}


def analyze_crawl_results(retailer_code, target_count, results_df):
    """
    Analyze crawling results

    Args:
        retailer_code: Retailer code (e.g., 'amazon', 'walmart', 'bestbuy')
        target_count: Total count of tracking list (target crawl count)
        results_df: Crawling results DataFrame

    Returns:
        dict: Analysis result
    """
    analysis = {
        'retailer_code': retailer_code,
        'retailer_name': RETAILER_NAMES.get(retailer_code, retailer_code.upper()),
        'target_count': target_count,
        'crawled_count': len(results_df) if results_df is not None else 0,
        'alerts': [],
        'is_critical': False,
        'has_countofreviews_error': False,  # count_of_star_ratings >= 100 but count_of_reviews is null
        'countofreviews_error_urls': [],  # URLs with this issue
        'has_price_error': False,  # final_sku_price is null
        'price_error_urls': [],  # URLs with price error
        'has_countofstarratings_error': False,  # star_rating exists but count_of_star_ratings is null
        'countofstarratings_error_urls': [],  # URLs with this issue
        'has_rv_detail_null_error': False,  # count_of_reviews > 0 but detailed_review_content is null
        'rv_detail_null_urls': [],  # URLs with this issue
        'field_stats': {}
    }

    # Crawling failed
    if results_df is None or len(results_df) == 0:
        analysis['alerts'].append({
            'type': 'CRITICAL',
            'message': 'Crawling execution failed - No result data (possible chromedriver error)'
        })
        analysis['is_critical'] = True
        return analysis

    crawled_count = len(results_df)

    # Check missing crawl attempts
    if crawled_count < target_count:
        missing_count = target_count - crawled_count
        analysis['alerts'].append({
            'type': 'CRITICAL',
            'message': f'Missing crawl attempts: {crawled_count} of {target_count} attempted ({missing_count} missing)'
        })
        analysis['is_critical'] = True

    # Analyze empty value ratio for each field (all tv_retail_com columns)
    fields_to_check = [
        'retailer_sku_name', 'star_rating', 'count_of_star_ratings', 'count_of_reviews',
        'screen_size', 'sku_popularity', 'final_sku_price', 'original_sku_price',
        'savings', 'discount_type', 'offer', 'pick_up_availability',
        'fastest_delivery', 'delivery_availability', 'shipping_info',
        'available_quantity_for_purchase', 'inventory_status', 'sku_status',
        'retailer_membership_discounts', 'detailed_review_content', 'summarized_review_content',
        'top_mentions', 'recommendation_intent', 'main_rank', 'bsr_rank', 'trend_rank',
        'rank_1', 'rank_2', 'promotion_position', 'number_of_ppl_purchased_yesterday',
        'number_of_ppl_added_to_carts', 'number_of_units_purchased_past_month',
        'retailer_sku_name_similar', 'estimated_annual_electricity_use', 'promotion_type', 'model_year'
    ]
    field_names = {
        'retailer_sku_name': 'Retailer SKU Name',
        'star_rating': 'Star Rating',
        'count_of_star_ratings': 'Count of Star Ratings',
        'count_of_reviews': 'Count of Reviews',
        'screen_size': 'Screen Size',
        'sku_popularity': 'SKU Popularity',
        'final_sku_price': 'Final SKU Price',
        'original_sku_price': 'Original SKU Price',
        'savings': 'Savings',
        'discount_type': 'Discount Type',
        'offer': 'Offer',
        'pick_up_availability': 'Pick Up Availability',
        'fastest_delivery': 'Fastest Delivery',
        'delivery_availability': 'Delivery Availability',
        'shipping_info': 'Shipping Info',
        'available_quantity_for_purchase': 'Available Quantity',
        'inventory_status': 'Inventory Status',
        'sku_status': 'SKU Status',
        'retailer_membership_discounts': 'Membership Discounts',
        'detailed_review_content': 'Detailed Review',
        'summarized_review_content': 'Summarized Review',
        'top_mentions': 'Top Mentions',
        'recommendation_intent': 'Recommendation Intent',
        'main_rank': 'Main Rank',
        'bsr_rank': 'BSR Rank',
        'trend_rank': 'Trend Rank',
        'rank_1': 'Rank 1',
        'rank_2': 'Rank 2',
        'promotion_position': 'Promotion Position',
        'number_of_ppl_purchased_yesterday': 'Purchased Yesterday',
        'number_of_ppl_added_to_carts': 'Added to Carts',
        'number_of_units_purchased_past_month': 'Units Purchased (Month)',
        'retailer_sku_name_similar': 'Similar SKU Name',
        'estimated_annual_electricity_use': 'Annual Electricity Use',
        'promotion_type': 'Promotion Type',
        'model_year': 'Model Year'
    }

    for field in fields_to_check:
        if field in results_df.columns:
            # Treat None, NaN, empty string as empty values
            empty_count = results_df[field].isna().sum() + (results_df[field] == '').sum()
            empty_rate = (empty_count / crawled_count) * 100 if crawled_count > 0 else 0

            analysis['field_stats'][field] = {
                'name': field_names.get(field, field),
                'empty_count': int(empty_count),
                'total_count': crawled_count,
                'empty_rate': round(empty_rate, 1)
            }

            # 50% or more empty values = CRITICAL alert
            if empty_rate >= 50:
                analysis['alerts'].append({
                    'type': 'CRITICAL',
                    'message': f'{field_names.get(field, field)} empty {empty_rate:.1f}% ({empty_count}/{crawled_count})'
                })
                analysis['is_critical'] = True
            # Any empty values = WARNING alert
            elif empty_count > 0:
                analysis['alerts'].append({
                    'type': 'WARNING',
                    'message': f'{field_names.get(field, field)} empty {empty_rate:.1f}% ({empty_count}/{crawled_count})'
                })

    # Check for count_of_star_ratings >= 100 but count_of_reviews is null
    if results_df is not None and len(results_df) > 0:
        if 'count_of_star_ratings' in results_df.columns and 'count_of_reviews' in results_df.columns:
            for idx, row in results_df.iterrows():
                star_ratings = row.get('count_of_star_ratings')
                reviews = row.get('count_of_reviews')

                # Check if count_of_star_ratings >= 100 and count_of_reviews is null/NaN
                try:
                    star_ratings_valid = star_ratings is not None and not pd.isna(star_ratings) and int(float(star_ratings)) >= 100
                except (ValueError, TypeError):
                    star_ratings_valid = False

                reviews_is_null = reviews is None or pd.isna(reviews)

                if star_ratings_valid and reviews_is_null:
                    analysis['has_countofreviews_error'] = True
                    url = row.get('product_url', 'N/A')
                    analysis['countofreviews_error_urls'].append(url)

    # Check for final_sku_price is null (price error)
    if results_df is not None and len(results_df) > 0:
        if 'final_sku_price' in results_df.columns:
            for idx, row in results_df.iterrows():
                price = row.get('final_sku_price')
                price_is_null = price is None or pd.isna(price) or (isinstance(price, str) and price.strip() == '')

                if price_is_null:
                    analysis['has_price_error'] = True
                    url = row.get('product_url', 'N/A')
                    analysis['price_error_urls'].append(url)

    # Check for star_rating exists but count_of_star_ratings is null
    if results_df is not None and len(results_df) > 0:
        if 'star_rating' in results_df.columns and 'count_of_star_ratings' in results_df.columns:
            for idx, row in results_df.iterrows():
                star_rating = row.get('star_rating')
                count_of_star_ratings = row.get('count_of_star_ratings')

                # Check if star_rating exists and is not "No ratings yet"
                star_rating_exists = (
                    star_rating is not None and
                    not pd.isna(star_rating) and
                    str(star_rating).strip() != '' and
                    str(star_rating).strip().lower() != 'no ratings yet'
                )

                # Check if count_of_star_ratings is null
                count_is_null = count_of_star_ratings is None or pd.isna(count_of_star_ratings)

                if star_rating_exists and count_is_null:
                    analysis['has_countofstarratings_error'] = True
                    url = row.get('product_url', 'N/A')
                    analysis['countofstarratings_error_urls'].append(url)

    # Check for count_of_reviews > 0 but detailed_review_content is null
    if results_df is not None and len(results_df) > 0:
        if 'count_of_reviews' in results_df.columns and 'detailed_review_content' in results_df.columns:
            for idx, row in results_df.iterrows():
                count_of_reviews = row.get('count_of_reviews')
                detailed_review_content = row.get('detailed_review_content')

                # Check if count_of_reviews > 0
                try:
                    count_valid = count_of_reviews is not None and not pd.isna(count_of_reviews) and int(float(str(count_of_reviews).replace(',', ''))) > 0
                except (ValueError, TypeError):
                    count_valid = False

                # Check if detailed_review_content is null/empty
                review_is_null = detailed_review_content is None or pd.isna(detailed_review_content) or (isinstance(detailed_review_content, str) and detailed_review_content.strip() == '')

                if count_valid and review_is_null:
                    analysis['has_rv_detail_null_error'] = True
                    url = row.get('product_url', 'N/A')
                    analysis['rv_detail_null_urls'].append(url)

    return analysis


def send_alert_email(analysis, error_message=None):
    """
    Send analysis result via email

    Args:
        analysis: Return value of analyze_crawl_results()
        error_message: Additional error message (optional)

    Returns:
        bool: Send success status
    """
    try:
        korea_tz = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_tz)

        # Generate email subject
        retailer_name = analysis['retailer_name']

        # Error prefixes
        error_prefixes = []
        if analysis.get('has_drv_20_error', False):
            error_prefixes.append("drv 20 error")
        if analysis.get('has_fsp_null_error', False):
            error_prefixes.append("fsp_null")
        if analysis.get('has_cosr_null_error', False):
            error_prefixes.append("cosr_null")
        if analysis.get('has_rv_detail_null_error', False):
            error_prefixes.append("rv_detail_null")
        if analysis.get('has_reviews_equals_ratings_error', False):
            error_prefixes.append("reviews_equals_ratings")
        if analysis.get('has_price_error', False):
            error_prefixes.append("price")
        if analysis.get('has_countofreviews_error', False):
            error_prefixes.append("countofreviews error")
        if analysis.get('has_countofstarratings_error', False):
            error_prefixes.append("countofstarratings error")
        prefix = " ".join(error_prefixes) + " " if error_prefixes else ""

        if analysis['is_critical'] or error_message:
            subject = f"{prefix}[CRITICAL] {retailer_name} TV Crawling Alert - {now.strftime('%Y-%m-%d %H:%M')}"
        elif analysis['alerts']:
            subject = f"{prefix}[WARNING] {retailer_name} TV Crawling Alert - {now.strftime('%Y-%m-%d %H:%M')}"
        else:
            subject = f"{prefix}[OK] {retailer_name} TV Crawling Report - {now.strftime('%Y-%m-%d %H:%M')}"

        # Generate email body (HTML)
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Malgun Gothic', Arial, sans-serif; }}
                .critical {{ color: #dc3545; font-weight: bold; }}
                .warning {{ color: #ffc107; font-weight: bold; }}
                table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .header {{ background-color: #333; color: white; padding: 15px; }}
                .section {{ margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>{retailer_name} TV Crawling Monitoring Report</h2>
                <p>Time: {now.strftime('%Y-%m-%d %H:%M:%S')} (KST)</p>
            </div>

            <div class="section">
                <h3>Crawling Status</h3>
                <table>
                    <tr>
                        <th>Item</th>
                        <th>Value</th>
                    </tr>
                    <tr>
                        <td>Target Product Count (Tracking List)</td>
                        <td>{analysis['target_count']}</td>
                    </tr>
                    <tr>
                        <td>Crawled Count</td>
                        <td>{analysis['crawled_count']}</td>
                    </tr>
                </table>
            </div>
        """

        # Alert messages section
        if analysis['alerts'] or error_message:
            html_content += """
            <div class="section">
                <h3>Alert Details</h3>
                <ul>
            """

            if error_message:
                html_content += f'<li class="critical">[CRITICAL] {error_message}</li>'

            for alert in analysis['alerts']:
                alert_class = 'critical' if alert['type'] == 'CRITICAL' else 'warning'
                html_content += f'<li class="{alert_class}">[{alert["type"]}] {alert["message"]}</li>'

            html_content += """
                </ul>
            </div>
            """

        # Field statistics section
        if analysis['field_stats']:
            html_content += """
            <div class="section">
                <h3>Field Empty Value Status</h3>
                <table>
                    <tr>
                        <th>Field</th>
                        <th>Empty Count</th>
                        <th>Total Count</th>
                        <th>Empty Rate</th>
                        <th>Status</th>
                    </tr>
            """

            for field, stats in analysis['field_stats'].items():
                if stats['empty_rate'] >= 50:
                    status = '<span class="critical">CRITICAL</span>'
                elif stats['empty_count'] > 0:
                    status = '<span class="warning">WARNING</span>'
                else:
                    status = 'OK'
                html_content += f"""
                    <tr>
                        <td>{stats['name']}</td>
                        <td>{stats['empty_count']}</td>
                        <td>{stats['total_count']}</td>
                        <td>{stats['empty_rate']}%</td>
                        <td>{status}</td>
                    </tr>
                """

            html_content += """
                </table>
            </div>
            """

        # price error section (final_sku_price is null)
        if analysis.get('has_price_error', False) and analysis.get('price_error_urls'):
            html_content += """
            <div class="section">
                <h3 style="color: #dc3545;">Price Error</h3>
                <p>The following products have final_sku_price as NULL:</p>
                <ul>
            """
            for url in analysis['price_error_urls']:
                html_content += f'<li><a href="{url}">{url}</a></li>'
            html_content += """
                </ul>
            </div>
            """

        # countofreviews error section (count_of_star_ratings >= 100 but count_of_reviews is null)
        if analysis.get('has_countofreviews_error', False) and analysis.get('countofreviews_error_urls'):
            html_content += """
            <div class="section">
                <h3 style="color: #dc3545;">Count of Reviews Error</h3>
                <p>The following products have count_of_star_ratings >= 100 but count_of_reviews is NULL:</p>
                <ul>
            """
            for url in analysis['countofreviews_error_urls']:
                html_content += f'<li><a href="{url}">{url}</a></li>'
            html_content += """
                </ul>
            </div>
            """

        # countofstarratings error section (star_rating exists but count_of_star_ratings is null)
        if analysis.get('has_countofstarratings_error', False) and analysis.get('countofstarratings_error_urls'):
            html_content += """
            <div class="section">
                <h3 style="color: #dc3545;">Count of Star Ratings Error</h3>
                <p>The following products have star_rating but count_of_star_ratings is NULL:</p>
                <ul>
            """
            for url in analysis['countofstarratings_error_urls']:
                html_content += f'<li><a href="{url}">{url}</a></li>'
            html_content += """
                </ul>
            </div>
            """

        # rv_detail_null error section (count_of_reviews > 0 but detailed_review_content is null)
        # Use crawler-provided records if available (includes account info)
        rv_records = analysis.get('rv_detail_null_records', [])
        if analysis.get('has_rv_detail_null_error', False) and rv_records:
            html_content += f"""
            <div class="section">
                <h3 style="color: #dc3545;">Review Detail Null Error ({len(rv_records)} products)</h3>
                <p>The following products have count_of_reviews > 0 but detailed_review_content is NULL:</p>
                <table>
                    <tr>
                        <th>Product URL</th>
                        <th>Reviews</th>
                        <th>Ratings</th>
                        <th>Account</th>
                    </tr>
            """
            for record in rv_records:
                url = record.get('url', 'N/A')
                cor = record.get('count_of_reviews', 'N/A')
                cosr = record.get('count_of_star_ratings', 'N/A')
                account = record.get('account', 'N/A')
                html_content += f"""
                    <tr>
                        <td><a href="{url}">{url[:80]}...</a></td>
                        <td>{cor}</td>
                        <td>{cosr}</td>
                        <td>{account}</td>
                    </tr>
                """
            html_content += """
                </table>
            </div>
            """
        # Fallback to old format (without account info)
        elif analysis.get('has_rv_detail_null_error', False) and analysis.get('rv_detail_null_urls'):
            html_content += f"""
            <div class="section">
                <h3 style="color: #dc3545;">Review Detail Null Error ({len(analysis['rv_detail_null_urls'])} products)</h3>
                <p>The following products have count_of_reviews > 0 but detailed_review_content is NULL:</p>
                <ul>
            """
            for url in analysis['rv_detail_null_urls']:
                html_content += f'<li><a href="{url}">{url}</a></li>'
            html_content += """
                </ul>
            </div>
            """

        # reviews_equals_ratings error section (count_of_reviews == count_of_star_ratings)
        rer_records = analysis.get('reviews_equals_ratings_records', [])
        if analysis.get('has_reviews_equals_ratings_error', False) and rer_records:
            html_content += f"""
            <div class="section">
                <h3 style="color: #dc3545;">Reviews Equals Ratings Error ({len(rer_records)} products)</h3>
                <p>The following products have count_of_reviews == count_of_star_ratings (suspicious):</p>
                <table>
                    <tr>
                        <th>Product URL</th>
                        <th>Reviews</th>
                        <th>Ratings</th>
                        <th>Account</th>
                    </tr>
            """
            for record in rer_records:
                url = record.get('url', 'N/A')
                cor = record.get('count_of_reviews', 'N/A')
                cosr = record.get('count_of_star_ratings', 'N/A')
                account = record.get('account', 'N/A')
                html_content += f"""
                    <tr>
                        <td><a href="{url}">{url[:80]}...</a></td>
                        <td>{cor}</td>
                        <td>{cosr}</td>
                        <td>{account}</td>
                    </tr>
                """
            html_content += """
                </table>
            </div>
            """

        # fsp_null error section (final_sku_price is null)
        fsp_records = analysis.get('fsp_null_records', [])
        if analysis.get('has_fsp_null_error', False) and fsp_records:
            html_content += f"""
            <div class="section">
                <h3 style="color: #dc3545;">Final SKU Price Null Error ({len(fsp_records)} products)</h3>
                <p>The following products have final_sku_price as NULL:</p>
                <table>
                    <tr>
                        <th>Product URL</th>
                        <th>Account</th>
                    </tr>
            """
            for record in fsp_records:
                url = record.get('url', 'N/A')
                account = record.get('account', 'N/A')
                html_content += f"""
                    <tr>
                        <td><a href="{url}">{url[:80]}...</a></td>
                        <td>{account}</td>
                    </tr>
                """
            html_content += """
                </table>
            </div>
            """

        # cosr_null error section (count_of_star_ratings is null but star_rating exists)
        cosr_records = analysis.get('cosr_null_records', [])
        if analysis.get('has_cosr_null_error', False) and cosr_records:
            html_content += f"""
            <div class="section">
                <h3 style="color: #dc3545;">Count of Star Ratings Null Error ({len(cosr_records)} products)</h3>
                <p>The following products have star_rating but count_of_star_ratings is NULL:</p>
                <table>
                    <tr>
                        <th>Product URL</th>
                        <th>Star Rating</th>
                        <th>Account</th>
                    </tr>
            """
            for record in cosr_records:
                url = record.get('url', 'N/A')
                star_rating = record.get('star_rating', 'N/A')
                account = record.get('account', 'N/A')
                html_content += f"""
                    <tr>
                        <td><a href="{url}">{url[:80]}...</a></td>
                        <td>{star_rating}</td>
                        <td>{account}</td>
                    </tr>
                """
            html_content += """
                </table>
            </div>
            """

        # drv_20_error section (detailed reviews under-collected)
        drv_20_records = analysis.get('drv_20_error_records', [])
        if analysis.get('has_drv_20_error', False) and drv_20_records:
            html_content += f"""
            <div class="section">
                <h3 style="color: #dc3545;">Detailed Reviews Under-Collected Error ({len(drv_20_records)} products)</h3>
                <p>Reviews under-collected (if total <= 20: collect all, if total > 20: collect at least 20):</p>
                <table>
                    <tr>
                        <th>Product URL</th>
                        <th>Total Reviews</th>
                        <th>Expected</th>
                        <th>Collected</th>
                    </tr>
            """
            for record in drv_20_records:
                url = record.get('url', 'N/A')
                total = record.get('count_of_reviews', 'N/A')
                expected = record.get('expected_count', record.get('count_of_reviews', 'N/A'))
                collected = record.get('collected_count', 'N/A')
                html_content += f"""
                    <tr>
                        <td><a href="{url}">{url[:80]}...</a></td>
                        <td>{total}</td>
                        <td>{expected}</td>
                        <td>{collected}</td>
                    </tr>
                """
            html_content += """
                </table>
            </div>
            """

        # screen_size_mismatch section
        ss_mismatch_records = analysis.get('screen_size_mismatch_records', [])
        if analysis.get('has_screen_size_mismatch', False) and ss_mismatch_records:
            html_content += f"""
            <div class="section">
                <h3 style="color: #ff6600;">Screen Size Mismatch Warning ({len(ss_mismatch_records)} items)</h3>
                <p>The following items have different screen_size values between extracted and tv_item_mst:</p>
                <table>
                    <tr>
                        <th>Item</th>
                        <th>Extracted</th>
                        <th>tv_item_mst</th>
                        <th>URL</th>
                    </tr>
            """
            for record in ss_mismatch_records:
                item = record.get('item', 'N/A')
                extracted = record.get('extracted', 'N/A')
                mst_value = record.get('mst_value', 'N/A')
                url = record.get('url', 'N/A')
                html_content += f"""
                    <tr>
                        <td>{item}</td>
                        <td>{extracted}</td>
                        <td>{mst_value}</td>
                        <td><a href="{url}">{url[:60]}...</a></td>
                    </tr>
                """
            html_content += """
                </table>
            </div>
            """

        # electricity_use_mismatch section
        elec_mismatch_records = analysis.get('electricity_use_mismatch_records', [])
        if analysis.get('has_electricity_use_mismatch', False) and elec_mismatch_records:
            html_content += f"""
            <div class="section">
                <h3 style="color: #ff6600;">Electricity Use Mismatch Warning ({len(elec_mismatch_records)} items)</h3>
                <p>The following items have different electricity_use values between extracted and tv_item_mst:</p>
                <table>
                    <tr>
                        <th>Item</th>
                        <th>Extracted</th>
                        <th>tv_item_mst</th>
                        <th>URL</th>
                    </tr>
            """
            for record in elec_mismatch_records:
                item = record.get('item', 'N/A')
                extracted = record.get('extracted', 'N/A')
                mst_value = record.get('mst_value', 'N/A')
                url = record.get('url', 'N/A')
                html_content += f"""
                    <tr>
                        <td>{item}</td>
                        <td>{extracted}</td>
                        <td>{mst_value}</td>
                        <td><a href="{url}">{url[:60]}...</a></td>
                    </tr>
                """
            html_content += """
                </table>
            </div>
            """

        html_content += """
            <div class="section">
                <p style="color: #666; font-size: 12px;">
                    This email was sent automatically. If issues persist, please check HTML/XPath changes.
                </p>
            </div>
        </body>
        </html>
        """

        # Create email
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = EMAIL_CONFIG['receiver_email']

        msg.attach(MIMEText(html_content, 'html'))

        # Send email
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            server.sendmail(
                EMAIL_CONFIG['sender_email'],
                EMAIL_CONFIG['receiver_email'],
                msg.as_string()
            )

        logger.info(f"Alert email sent: {subject}")
        return True

    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False


def monitor_and_alert(retailer_code, target_count, results_df, error_message=None,
                      rv_detail_null_records=None, reviews_equals_ratings_records=None,
                      fsp_null_records=None, cosr_null_records=None, drv_20_error_records=None,
                      screen_size_mismatch_records=None, electricity_use_mismatch_records=None):
    """
    Monitor crawling results and send alerts (main function)

    Call this function from each crawler.

    Args:
        retailer_code: Retailer code
        target_count: Total tracking list count
        results_df: Crawling results DataFrame (None if failed)
        error_message: Additional error message (optional)
        rv_detail_null_records: List of dicts with url, count_of_reviews, count_of_star_ratings, account
        reviews_equals_ratings_records: List of dicts with url, count_of_reviews, count_of_star_ratings, account
        fsp_null_records: List of dicts with url, account (final_sku_price is null)
        cosr_null_records: List of dicts with url, star_rating, account (count_of_star_ratings is null)
        drv_20_error_records: List of dicts with url, count_of_reviews, collected_count (detailed reviews under-collected)

    Returns:
        bool: Alert send success status

    Usage example:
        from alert_monitor import monitor_and_alert

        # After crawling complete
        monitor_and_alert('amazon', len(urls_data), results_df)

        # On error
        monitor_and_alert('amazon', len(urls_data), None, error_message="ChromeDriver init failed")
    """
    try:
        # Analyze results
        analysis = analyze_crawl_results(retailer_code, target_count, results_df)

        # Override with crawler-provided records (includes account info)
        if rv_detail_null_records is not None:
            analysis['rv_detail_null_records'] = rv_detail_null_records
            analysis['has_rv_detail_null_error'] = len(rv_detail_null_records) > 0

        if reviews_equals_ratings_records is not None:
            analysis['reviews_equals_ratings_records'] = reviews_equals_ratings_records
            analysis['has_reviews_equals_ratings_error'] = len(reviews_equals_ratings_records) > 0

        if fsp_null_records is not None:
            analysis['fsp_null_records'] = fsp_null_records
            analysis['has_fsp_null_error'] = len(fsp_null_records) > 0

        if cosr_null_records is not None:
            analysis['cosr_null_records'] = cosr_null_records
            analysis['has_cosr_null_error'] = len(cosr_null_records) > 0

        if drv_20_error_records is not None:
            analysis['drv_20_error_records'] = drv_20_error_records
            analysis['has_drv_20_error'] = len(drv_20_error_records) > 0

        if screen_size_mismatch_records is not None:
            analysis['screen_size_mismatch_records'] = screen_size_mismatch_records
            analysis['has_screen_size_mismatch'] = len(screen_size_mismatch_records) > 0

        if electricity_use_mismatch_records is not None:
            analysis['electricity_use_mismatch_records'] = electricity_use_mismatch_records
            analysis['has_electricity_use_mismatch'] = len(electricity_use_mismatch_records) > 0

        # Always send email (daily report)
        return send_alert_email(analysis, error_message)

    except Exception as e:
        logger.error(f"Monitoring error: {e}")
        return False


def send_review_url_error_alert(product_url, expected_sku, actual_sku):
    """
    Send alert when review page SKU doesn't match product page SKU.

    Args:
        product_url: Original product page URL
        expected_sku: SKU extracted from product page
        actual_sku: SKU found in review page URL

    Returns:
        bool: Email send success status
    """
    try:
        korea_tz = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_tz)

        subject = "[DX]bby_tv_review url error"

        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Malgun Gothic', Arial, sans-serif; }}
                .error {{ color: #dc3545; font-weight: bold; }}
                table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #dc3545; color: white; }}
                .header {{ background-color: #333; color: white; padding: 15px; }}
                .section {{ margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>BestBuy TV Review URL Error</h2>
                <p>Time: {now.strftime('%Y-%m-%d %H:%M:%S')} (KST)</p>
            </div>

            <div class="section">
                <h3 class="error">SKU Mismatch Detected</h3>
                <p>The review page SKU does not match the product page SKU. This may result in collecting reviews from a different product.</p>
                <table>
                    <tr>
                        <th>Item</th>
                        <th>Value</th>
                    </tr>
                    <tr>
                        <td>Product URL</td>
                        <td><a href="{product_url}">{product_url}</a></td>
                    </tr>
                    <tr>
                        <td>Expected SKU (from product page)</td>
                        <td>{expected_sku}</td>
                    </tr>
                    <tr>
                        <td>Actual SKU (from review URL)</td>
                        <td>{actual_sku}</td>
                    </tr>
                </table>
            </div>

            <div class="section">
                <p style="color: #666; font-size: 12px;">
                    This email was sent automatically. Please check the product page and review page URLs.
                </p>
            </div>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = EMAIL_CONFIG['receiver_email']

        msg.attach(MIMEText(html_content, 'html'))

        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            server.sendmail(
                EMAIL_CONFIG['sender_email'],
                EMAIL_CONFIG['receiver_email'],
                msg.as_string()
            )

        logger.info(f"Review URL error alert sent: {subject}")
        return True

    except Exception as e:
        logger.error(f"Failed to send review URL error alert: {e}")
        return False


def send_crawl_alert(retailer, results, failed_stages, elapsed_time, error_message=None):
    """
    Send crawling completion/failure email alert for integrated crawlers.

    Args:
        retailer: Retailer name (e.g., 'Amazon', 'Walmart', 'BestBuy')
        results: Dictionary of stage results {stage_name: success_bool or {'success': bool, 'duration': float}}
        failed_stages: List of failed stage names
        elapsed_time: Total elapsed time in seconds
        error_message: Additional error message (optional)

    Returns:
        bool: Email send success status

    Usage example:
        from alert_monitor import send_crawl_alert

        # After crawling complete
        send_crawl_alert(
            retailer='Amazon',
            results={'main1': True, 'bsr1': False, 'dt1': True},
            failed_stages=['bsr1'],
            elapsed_time=3600.5
        )

        # On fatal error
        send_crawl_alert(
            retailer='Amazon',
            results={},
            failed_stages=['Fatal error'],
            elapsed_time=0,
            error_message='ChromeDriver initialization failed'
        )
    """
    try:
        korea_tz = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_tz)

        # Determine alert level
        is_critical = len(failed_stages) > 0 or error_message is not None

        # Generate email subject
        if is_critical:
            subject = f"[CRITICAL] {retailer} TV Crawler Alert - {now.strftime('%Y-%m-%d %H:%M')}"
        else:
            subject = f"[OK] {retailer} TV Crawler Report - {now.strftime('%Y-%m-%d %H:%M')}"

        # Build results table rows
        results_rows = ""
        for stage_name, result in results.items():
            # Handle both dict format {'success': bool, 'duration': float} and simple bool format
            if isinstance(result, dict):
                success = result.get('success', False)
                duration = result.get('duration')
                if success is None:
                    status = '<span style="color: #6c757d;">SKIPPED</span>'
                elif success:
                    status = '<span style="color: #28a745;">SUCCESS</span>'
                else:
                    status = '<span style="color: #dc3545;">FAILED</span>'
                duration_str = f"{duration:.1f}s" if duration is not None else "N/A"
            else:
                # Simple bool or None
                if result is None:
                    status = '<span style="color: #6c757d;">SKIPPED</span>'
                elif result:
                    status = '<span style="color: #28a745;">SUCCESS</span>'
                else:
                    status = '<span style="color: #dc3545;">FAILED</span>'
                duration_str = "N/A"

            results_rows += f"""
                <tr>
                    <td>{stage_name}</td>
                    <td>{status}</td>
                    <td>{duration_str}</td>
                </tr>
            """

        # Generate email body (HTML)
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Malgun Gothic', Arial, sans-serif; }}
                .critical {{ color: #dc3545; font-weight: bold; }}
                .success {{ color: #28a745; font-weight: bold; }}
                table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .header {{ background-color: #333; color: white; padding: 15px; }}
                .section {{ margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>{retailer} TV Crawler Report</h2>
                <p>Time: {now.strftime('%Y-%m-%d %H:%M:%S')} (KST)</p>
            </div>

            <div class="section">
                <h3>Execution Summary</h3>
                <table>
                    <tr>
                        <th>Item</th>
                        <th>Value</th>
                    </tr>
                    <tr>
                        <td>Total Elapsed Time</td>
                        <td>{elapsed_time:.1f} seconds ({elapsed_time/60:.1f} minutes)</td>
                    </tr>
                    <tr>
                        <td>Overall Status</td>
                        <td>{'<span class="critical">FAILED</span>' if is_critical else '<span class="success">SUCCESS</span>'}</td>
                    </tr>
                </table>
            </div>

            <div class="section">
                <h3>Stage Results</h3>
                <table>
                    <tr>
                        <th>Stage</th>
                        <th>Status</th>
                        <th>Duration</th>
                    </tr>
                    {results_rows}
                </table>
            </div>
        """

        # Add error message section if present
        if error_message:
            html_content += f"""
            <div class="section">
                <h3>Error Details</h3>
                <div style="background-color: #f8d7da; border: 1px solid #f5c6cb; border-radius: 4px; padding: 10px; color: #721c24;">
                    {error_message}
                </div>
            </div>
            """

        # Add failed stages section if present
        if failed_stages:
            failed_list = "".join([f"<li>{stage}</li>" for stage in failed_stages])
            html_content += f"""
            <div class="section">
                <h3>Failed Stages</h3>
                <ul class="critical">
                    {failed_list}
                </ul>
            </div>
            """

        html_content += """
            <div class="section">
                <p style="color: #666; font-size: 12px;">
                    This email was sent automatically. If issues persist, please check the crawler logs.
                </p>
            </div>
        </body>
        </html>
        """

        # Create email
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = EMAIL_CONFIG['receiver_email']

        msg.attach(MIMEText(html_content, 'html'))

        # Send email
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            server.sendmail(
                EMAIL_CONFIG['sender_email'],
                EMAIL_CONFIG['receiver_email'],
                msg.as_string()
            )

        logger.info(f"Crawl alert email sent: {subject}")
        return True

    except Exception as e:
        logger.error(f"Failed to send crawl alert email: {e}")
        return False


def send_sku_renewed_alert(retailer_name, sku_updated_records):
    """
    Send SKU renewed alert email with CSV attachment.

    Args:
        retailer_name: Retailer display name (e.g., 'Amazon TV', 'Walmart TV', 'BestBuy TV')
        sku_updated_records: List of dicts with account_name, item, product_url, old_sku, new_sku

    Returns:
        bool: Email send success status
    """
    if not sku_updated_records:
        return False

    try:
        korea_tz = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_tz)

        renewed_count = len(sku_updated_records)
        subject = f"[sku_renewed] {retailer_name} {renewed_count} - {now.strftime('%Y-%m-%d %H:%M')}"

        # Build HTML table
        table_rows = ""
        for record in sku_updated_records:
            table_rows += f"""
                <tr>
                    <td>{record.get('account_name', 'N/A')}</td>
                    <td>{record.get('item', 'N/A')}</td>
                    <td><a href="{record.get('product_url', '#')}">{(record.get('product_url', 'N/A'))[:80]}...</a></td>
                    <td>{record.get('old_sku', 'N/A')}</td>
                    <td>{record.get('new_sku', 'N/A')}</td>
                </tr>
            """

        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Malgun Gothic', Arial, sans-serif; }}
                table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .header {{ background-color: #333; color: white; padding: 15px; }}
                .section {{ margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>{retailer_name} SKU Renewed Report</h2>
                <p>Time: {now.strftime('%Y-%m-%d %H:%M:%S')} (KST)</p>
            </div>

            <div class="section">
                <h3>SKU Renewed: {renewed_count} items</h3>
                <table>
                    <tr>
                        <th>Account</th>
                        <th>Item</th>
                        <th>Product URL</th>
                        <th>Old SKU</th>
                        <th>New SKU</th>
                    </tr>
                    {table_rows}
                </table>
            </div>

            <div class="section">
                <p style="color: #666; font-size: 12px;">
                    This email was sent automatically. CSV file is attached.
                </p>
            </div>
        </body>
        </html>
        """

        # Generate CSV in memory
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=['account_name', 'item', 'product_url', 'old_sku', 'new_sku'])
        writer.writeheader()
        for record in sku_updated_records:
            writer.writerow({
                'account_name': record.get('account_name', ''),
                'item': record.get('item', ''),
                'product_url': record.get('product_url', ''),
                'old_sku': record.get('old_sku', ''),
                'new_sku': record.get('new_sku', '')
            })
        csv_data = csv_buffer.getvalue()
        csv_buffer.close()

        # Create email (mixed: HTML + attachment)
        msg = MIMEMultipart('mixed')
        msg['Subject'] = subject
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = EMAIL_CONFIG['receiver_email']

        msg.attach(MIMEText(html_content, 'html'))

        # Attach CSV file
        csv_filename = f"sku_renewed_{retailer_name.replace(' ', '_')}_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        csv_part = MIMEBase('text', 'csv')
        csv_part.set_payload(csv_data.encode('utf-8-sig'))
        encoders.encode_base64(csv_part)
        csv_part.add_header('Content-Disposition', f'attachment; filename="{csv_filename}"')
        msg.attach(csv_part)

        # Send email
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            server.sendmail(
                EMAIL_CONFIG['sender_email'],
                EMAIL_CONFIG['receiver_email'],
                msg.as_string()
            )

        logger.info(f"SKU renewed alert sent: {subject}")
        return True

    except Exception as e:
        logger.error(f"Failed to send SKU renewed alert: {e}")
        return False


def _get_prev_session_data(db_conn, current_session_min, account_name='Walmart'):
    """
    DB에서 직전 세션 데이터 조회

    Args:
        db_conn: psycopg2 connection
        current_session_min: 현재 세션의 MIN(crawl_datetime) 문자열
        account_name: 'Walmart' 또는 'Amazon'

    Returns:
        (prev_df, prev_session_start): DataFrame과 직전 세션 시작시간, 없으면 (None, None)
    """
    try:
        cursor = db_conn.cursor()

        # 1) 현재 세션 이전의 가장 최근 crawl_datetime
        cursor.execute("""
            SELECT MAX(crawl_datetime) FROM tv_retail_com
            WHERE account_name = %s
            AND crawl_datetime::timestamp < %s::timestamp
        """, (account_name, current_session_min,))
        row = cursor.fetchone()
        if not row or not row[0]:
            cursor.close()
            return None, None
        prev_max = row[0]

        # 2) 직전 세션 시작시간 (prev_max 기준 12시간 이내)
        cursor.execute("""
            SELECT MIN(crawl_datetime) FROM tv_retail_com
            WHERE account_name = %s
            AND crawl_datetime::timestamp BETWEEN (%s::timestamp - INTERVAL '12 hours') AND %s::timestamp
        """, (account_name, prev_max, prev_max))
        row = cursor.fetchone()
        prev_session_start = row[0] if row and row[0] else prev_max

        # 3) 직전 세션 전체 데이터
        cursor.execute("""
            SELECT product_url, retailer_sku_name, star_rating, count_of_star_ratings, count_of_reviews,
                   screen_size, sku_popularity, final_sku_price, original_sku_price,
                   savings, discount_type, offer, pick_up_availability,
                   fastest_delivery, delivery_availability, shipping_info,
                   available_quantity_for_purchase, inventory_status, sku_status,
                   retailer_membership_discounts, detailed_review_content, main_rank, bsr_rank,
                   model_year
            FROM tv_retail_com
            WHERE account_name = %s
            AND crawl_datetime::timestamp BETWEEN %s::timestamp AND %s::timestamp
        """, (account_name, prev_session_start, prev_max))

        rows = cursor.fetchall()
        columns = [
            'product_url', 'retailer_sku_name', 'star_rating', 'count_of_star_ratings', 'count_of_reviews',
            'screen_size', 'sku_popularity', 'final_sku_price', 'original_sku_price',
            'savings', 'discount_type', 'offer', 'pick_up_availability',
            'fastest_delivery', 'delivery_availability', 'shipping_info',
            'available_quantity_for_purchase', 'inventory_status', 'sku_status',
            'retailer_membership_discounts', 'detailed_review_content', 'main_rank', 'bsr_rank',
            'model_year'
        ]
        cursor.close()

        if rows:
            return pd.DataFrame(rows, columns=columns), prev_session_start
        return None, None

    except Exception as e:
        logger.error(f"직전 세션 데이터 조회 실패: {e}")
        return None, None


def _get_current_session_data(db_conn, account_name='Walmart'):
    """
    DB에서 현재 세션 데이터 조회

    Returns:
        (curr_df, session_min): DataFrame과 현재 세션 MIN(crawl_datetime), 없으면 (None, None)
    """
    try:
        cursor = db_conn.cursor()

        # 현재 세션 시작시간 판별 (최근 12시간 이내)
        cursor.execute("""
            SELECT MIN(crawl_datetime) FROM tv_retail_com
            WHERE account_name = %s
            AND crawl_datetime::timestamp >= NOW() - INTERVAL '12 hours'
        """, (account_name,))
        row = cursor.fetchone()
        if not row or not row[0]:
            cursor.close()
            return None, None
        session_min = row[0]

        cursor.execute("""
            SELECT product_url, retailer_sku_name, star_rating, count_of_star_ratings, count_of_reviews,
                   screen_size, sku_popularity, final_sku_price, original_sku_price,
                   savings, discount_type, offer, pick_up_availability,
                   fastest_delivery, delivery_availability, shipping_info,
                   available_quantity_for_purchase, inventory_status, sku_status,
                   retailer_membership_discounts, detailed_review_content, main_rank, bsr_rank,
                   model_year
            FROM tv_retail_com
            WHERE account_name = %s
            AND crawl_datetime::timestamp >= %s::timestamp
        """, (account_name, session_min,))

        rows = cursor.fetchall()
        columns = [
            'product_url', 'retailer_sku_name', 'star_rating', 'count_of_star_ratings', 'count_of_reviews',
            'screen_size', 'sku_popularity', 'final_sku_price', 'original_sku_price',
            'savings', 'discount_type', 'offer', 'pick_up_availability',
            'fastest_delivery', 'delivery_availability', 'shipping_info',
            'available_quantity_for_purchase', 'inventory_status', 'sku_status',
            'retailer_membership_discounts', 'detailed_review_content', 'main_rank', 'bsr_rank',
            'model_year'
        ]
        cursor.close()

        if rows:
            return pd.DataFrame(rows, columns=columns), session_min
        return None, None

    except Exception as e:
        logger.error(f"현재 세션 데이터 조회 실패: {e}")
        return None, None


def _compare_sessions(curr_df, prev_df, retailer='Walmart'):
    """
    현재 세션 vs 직전 세션 필드별 비교

    Returns:
        dict: {
            'field_stats': {field: {empty_count, prev_empty_count, total}},
            'newly_empty': [{url, product_name, field}],
            'recovered': [{url, product_name, field}],
            'all_null_fields': [field_name, ...]  # 빈 값 비율 100%인 필드
        }
    """
    fields_to_check = [
        'retailer_sku_name', 'star_rating', 'count_of_star_ratings', 'count_of_reviews',
        'screen_size', 'final_sku_price', 'original_sku_price',
        'detailed_review_content', 'main_rank', 'bsr_rank'
    ]
    # Walmart 전용 필드 (Amazon에서는 항상 NULL이므로 제외)
    if retailer == 'Walmart':
        fields_to_check.extend(['pick_up_availability', 'fastest_delivery', 'delivery_availability'])

    result = {
        'field_stats': {},
        'newly_empty': [],
        'recovered': [],
        'all_null_fields': []  # 빈 값 비율 100%인 필드 목록
    }

    if curr_df is None or curr_df.empty:
        return result

    total = len(curr_df)

    # 필드별 빈 값 통계 + 세션 비교
    for field in fields_to_check:
        if field not in curr_df.columns:
            continue

        # 현재 빈 값
        curr_empty = curr_df[field].isna() | (curr_df[field].astype(str).isin(['', 'None', 'nan']))
        curr_empty_count = int(curr_empty.sum())
        empty_rate = round((curr_empty_count / total) * 100, 1) if total > 0 else 0

        stat = {
            'empty_count': curr_empty_count,
            'prev_empty_count': '-',
            'total': total,
            'empty_rate': empty_rate
        }

        # 빈 값 비율 100%인 필드 경고
        if curr_empty_count == total and total > 0:
            result['all_null_fields'].append(field)

        # 직전 세션 비교
        if prev_df is not None and not prev_df.empty and field in prev_df.columns:
            prev_empty = prev_df[field].isna() | (prev_df[field].astype(str).isin(['', 'None', 'nan']))
            stat['prev_empty_count'] = int(prev_empty.sum())

            # URL 기반 비교
            if 'product_url' in curr_df.columns and 'product_url' in prev_df.columns:
                curr_empty_urls = set(curr_df.loc[curr_empty, 'product_url'])
                prev_empty_urls = set(prev_df.loc[prev_empty, 'product_url'])
                prev_all_urls = set(prev_df['product_url'])
                curr_all_urls = set(curr_df['product_url'])

                # 신규 빈 값: 직전에 값 있었는데 지금 빈 값 (양쪽 모두 존재하는 URL만)
                newly_empty_urls = (curr_empty_urls - prev_empty_urls) & prev_all_urls
                for url in sorted(newly_empty_urls):
                    row = curr_df[curr_df['product_url'] == url].iloc[0]
                    name = row.get('retailer_sku_name', '')
                    name = str(name)[:50] if name and not pd.isna(name) else '(빈 값)'
                    result['newly_empty'].append({
                        'url': url, 'product_name': name, 'field': field
                    })

                # 값 복구: 직전에 빈 값이었는데 지금 값 있음
                recovered_urls = (prev_empty_urls - curr_empty_urls) & curr_all_urls
                for url in sorted(recovered_urls):
                    row = curr_df[curr_df['product_url'] == url].iloc[0]
                    name = row.get('retailer_sku_name', '')
                    name = str(name)[:50] if name and not pd.isna(name) else '(빈 값)'
                    result['recovered'].append({
                        'url': url, 'product_name': name, 'field': field
                    })

        result['field_stats'][field] = stat

    return result


def send_tv_crawl_report(retailer, stage_results, failed_stages, overall_elapsed,
                         stage_order=None, is_interim=False, error_message=None,
                         main_dedup=None):
    """
    TV 크롤링 리포트 이메일 발송 (Walmart/Amazon 공용)

    Args:
        retailer: 'Walmart' 또는 'Amazon'
        stage_results: {stage_name: {success, elapsed, timeout, collected_count, target_count}}
        failed_stages: 실패 스테이지 이름 리스트
        overall_elapsed: 총 소요시간 (초)
        stage_order: 스테이지 표시 순서 리스트 (선택)
        is_interim: True이면 6시간 중간보고
        error_message: 추가 에러 메시지 (선택)
        main_dedup: main1+main2 중복제거 URL 수 (Walmart에서만 사용, 없으면 표시 안 함)
    """
    try:
        korea_tz = pytz.timezone('Asia/Seoul')
        now = datetime.now(korea_tz)

        # DB 연결 (dt1 결과 비교용)
        import psycopg2
        from config import DB_CONFIG
        db_conn = None
        comparison = None
        prev_session_start = None

        # retailer별 설정
        account_name = retailer  # DB의 account_name과 동일 ('Walmart', 'Amazon')
        if not stage_order:
            if retailer == 'Walmart':
                stage_order = ['wmart_tv_main1', 'wmart_tv_main2', 'wmart_tv_bsr', 'wmart_tv_dt1']
            else:
                stage_order = ['amazon_tv_main1', 'amazon_tv_bsr1', 'amazon_tv_dt1']

        # dt 스테이지 이름 찾기 (dt1 또는 dt2)
        dt_stage = [s for s in stage_order if '_dt' in s]
        dt_stage_name = dt_stage[0] if dt_stage else None

        if not is_interim:
            try:
                db_conn = psycopg2.connect(**DB_CONFIG)
                db_conn.autocommit = True
                curr_df, session_min = _get_current_session_data(db_conn, account_name)
                if curr_df is not None and session_min:
                    prev_df, prev_session_start = _get_prev_session_data(db_conn, session_min, account_name)
                    comparison = _compare_sessions(curr_df, prev_df, retailer)
            except Exception as e:
                logger.error(f"DB 세션 비교 실패: {e}")
            finally:
                if db_conn:
                    db_conn.close()

        # 제목 생성
        failed_parts = []
        for stage in failed_stages:
            if dt_stage_name and stage == dt_stage_name:
                dt_short = dt_stage_name.replace('wmart_tv_', '').replace('amazon_tv_', '')
                dt_result = stage_results.get(dt_stage_name, {})
                dt_target = dt_result.get('target_count') or 0
                dt_collected = dt_result.get('collected_count') or 0
                if dt_result.get('timeout'):
                    failed_parts.append(f'{dt_short} 타임아웃')
                elif dt_target > 0:
                    failed_parts.append(f'{dt_short} {dt_target - dt_collected} sku')
                else:
                    failed_parts.append(dt_short)
            else:
                # 스테이지 이름에서 접두사 제거하여 간결하게 표시
                short = stage.replace('wmart_tv_', '').replace('amazon_tv_', '')
                failed_parts.append(short)

        # main/bsr 수집 수 집계 (스테이지 이름에서 동적으로 찾기)
        main_stages = [s for s in stage_order if 'main' in s]
        bsr_stages = [s for s in stage_order if 'bsr' in s]
        main_total = sum((stage_results.get(s, {}).get('collected_count') or 0) for s in main_stages)
        bsr_c = sum((stage_results.get(s, {}).get('collected_count') or 0) for s in bsr_stages)

        alerts = []
        main_min = 250 if retailer == 'Amazon' else 300
        if 0 < main_total < main_min:
            alerts.append(f'main 합산 {main_total}개 ({main_min} 미만)')

        # bsr 유실 판정: expected/excluded 필드 있으면 정식, 없으면 기존 100 미만 fallback
        # - expected: Amazon 노출한 실제 container 개수
        # - excluded: tv_item_mst.is_product=false 로 의도적으로 제외한 개수
        # - missed: expected - collected - excluded (실제 유실)
        bsr_expected = sum((stage_results.get(s, {}).get('expected_count') or 0) for s in bsr_stages)
        bsr_excluded = sum((stage_results.get(s, {}).get('excluded_count') or 0) for s in bsr_stages)
        if bsr_expected > 0:
            bsr_missed = bsr_expected - bsr_c - bsr_excluded
            if bsr_missed > 0:
                alerts.append(f'bsr 유실 {bsr_missed}개 (expected {bsr_expected}, collected {bsr_c}, excluded {bsr_excluded})')
        else:
            # expected 정보 없는 구 JSON (다른 retailer 등) — fallback: 기존 100 미만 기준
            if 0 < bsr_c < 100:
                alerts.append(f'bsr {bsr_c}개 (100 미만)')
        dt1_result = stage_results.get(dt_stage_name, {}) if dt_stage_name else {}
        dt1_target = dt1_result.get('target_count') or 0
        dt_target_min = 250 if retailer == 'Amazon' else 300
        if 0 < dt1_target < dt_target_min:
            alerts.append(f'dt 대상 {dt1_target}개 ({dt_target_min} 미만)')

        failed_prefix = f"Failed {' '.join(failed_parts)} " if failed_parts else ""

        if is_interim:
            subject = f"[DX_SEA] 진행중 {retailer} TV Crawler - 6시간 경과"
        else:
            subject = f"[DX_SEA] {failed_prefix}{retailer} TV Crawler {'완료' if not failed_parts else ''}"
            subject = subject.rstrip()

        # HTML 본문
        report_type = "중간 리포트" if is_interim else "크롤링 리포트"
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Malgun Gothic', Arial, sans-serif; }}
                .critical {{ color: #dc3545; font-weight: bold; }}
                .warning {{ color: #ffc107; font-weight: bold; }}
                .success {{ color: #28a745; font-weight: bold; }}
                .notice {{ color: #17a2b8; font-weight: bold; }}
                table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .header {{ background-color: #333; color: white; padding: 15px; }}
                .section {{ margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>{retailer} TV {report_type}</h2>
                <p>시간: {now.strftime('%Y-%m-%d %H:%M:%S')} (KST)</p>
            </div>

            <div class="section">
                <h3>실행 요약</h3>
                <table>
                    <tr><th>항목</th><th>값</th></tr>
                    <tr><td>총 소요시간</td><td>{overall_elapsed:.1f}초 ({overall_elapsed/60:.1f}분)</td></tr>
                </table>
            </div>

            <div class="section">
                <h3>스테이지별 결과</h3>
                <table>
                    <tr><th>스테이지</th><th>상태</th><th>수집 현황</th><th>소요시간</th></tr>
        """

        for name in stage_order:
            sr = stage_results.get(name, {})
            if not sr and is_interim:
                status = '<span class="notice">진행중</span>'
                sr = {}
            elif sr.get('success') is None:
                status = '<span style="color: #6c757d;">미실행</span>'
            else:
                collected = sr.get('collected_count') or 0
                is_dt = '_dt' in name
                is_main = 'main' in name

                if retailer == 'Amazon':
                    if is_main:
                        if collected >= 250:
                            status = '<span class="success">성공</span>'
                        elif collected > 0:
                            status = '<span class="warning">부족</span>'
                        else:
                            status = '<span class="critical">실패</span>'
                    elif is_dt:
                        if collected >= 201:
                            status = '<span class="success">성공</span>'
                        elif collected > 0:
                            status = '<span class="warning">부족</span>'
                        else:
                            status = '<span class="critical">실패</span>'
                    else:  # bsr — 오케스트레이터 판정(sr['success']) 우선, 없으면 collected>=100 fallback
                        if 'success' in sr and sr.get('success') is not None:
                            status = '<span class="success">성공</span>' if sr.get('success') else '<span class="critical">실패</span>'
                        else:
                            status = '<span class="success">성공</span>' if collected >= 100 else '<span class="critical">실패</span>'
                else:  # Walmart
                    if sr.get('success'):
                        status = '<span class="success">성공</span>'
                    else:
                        status = '<span class="critical">실패</span>'

            collected = sr.get('collected_count')
            target = sr.get('target_count')
            if collected is not None and target is not None:
                if collected >= target:
                    collected_str = f"{collected} sku (목표 {target} 달성)"
                else:
                    collected_str = f"{collected} sku / {target} sku"
            elif collected is not None:
                collected_str = f"{collected} url"
            else:
                collected_str = '-'

            if sr.get('timeout'):
                elapsed_str = '타임아웃'
            elif sr.get('elapsed', 0) > 0:
                elapsed_str = f"{sr['elapsed']:.1f}초"
            else:
                elapsed_str = '-'

            html_content += f"<tr><td>{name}</td><td>{status}</td><td>{collected_str}</td><td>{elapsed_str}</td></tr>"

        # main 합산 행 (main 스테이지가 2개 이상일 때만)
        if len(main_stages) >= 2:
            main_label = '+'.join(s.split('_')[-1] for s in main_stages)
            html_content += f"""
                    <tr style="background-color: #e9ecef; font-weight: bold;">
                        <td>{main_label} 합산</td><td></td><td>{main_total} url {'<span class="critical">(300 미만)</span>' if 0 < main_total < 300 else ''}</td><td></td>
                    </tr>"""
            # main 중복제거 행 (Walmart에서만, main_dedup 전달된 경우)
            if main_dedup is not None and main_dedup >= 0:
                html_content += f"""
                    <tr style="background-color: #e9ecef; font-weight: bold;">
                        <td>{main_label} 중복제거</td><td></td><td>{main_dedup} url {'<span class="critical">(300 미만)</span>' if 0 < main_dedup < 300 else ''}</td><td></td>
                    </tr>"""
        html_content += """
                </table>
            </div>
        """

        # 알림 내용
        if alerts or error_message:
            html_content += '<div class="section"><h3>알림 내용</h3><ul>'
            if error_message:
                html_content += f'<li class="critical">[CRITICAL] {error_message}</li>'
            for alert in alerts:
                html_content += f'<li class="warning">{alert}</li>'
            html_content += '</ul></div>'

        # 이하 dt1 완료 후 최종 보고에서만 표시
        if not is_interim and comparison:
            # 전필드 NULL 경고 (빈 값 비율 100%인 필드)
            all_null_fields = comparison.get('all_null_fields', [])
            if all_null_fields:
                html_content += f"""
                <div class="section">
                    <h3 class="warning">전필드 NULL 경고 ({len(all_null_fields)}개 필드)</h3>
                    <p style="font-size: 12px; color: #666;">아래 필드는 전체 row에서 빈 값 비율이 100%입니다</p>
                    <ul>
                """
                for field in all_null_fields:
                    html_content += f"<li class='warning'>{field}</li>"
                html_content += "</ul></div>"

            # 필드별 빈 값 현황
            field_stats = comparison.get('field_stats', {})
            if field_stats:
                prev_label = f"직전 세션"
                if prev_session_start:
                    prev_label = f"직전 세션 ({prev_session_start})"

                html_content += f"""
                <div class="section">
                    <h3>필드별 빈 값 현황 ({dt_stage_name or 'dt'})</h3>
                    <table>
                        <tr><th>필드명</th><th>빈 값 개수</th><th>{prev_label}</th><th>차이</th><th>총 개수</th><th>빈 값 비율</th><th>상태</th></tr>
                """
                for field, stat in field_stats.items():
                    prev_empty = stat['prev_empty_count']
                    if prev_empty != '-':
                        diff = stat['empty_count'] - prev_empty
                        if diff > 0:
                            diff_str = f'<span class="critical">+{diff}</span>'
                        elif diff < 0:
                            diff_str = f'<span class="success">{diff}</span>'
                        else:
                            diff_str = '0'
                    else:
                        diff_str = '-'

                    if stat['empty_rate'] >= 20:
                        status = '<span class="critical">do check</span>'
                    else:
                        status = '<span class="success">정상</span>'

                    html_content += f"""
                        <tr><td>{field}</td><td>{stat['empty_count']}</td><td>{prev_empty}</td>
                        <td>{diff_str}</td><td>{stat['total']}</td><td>{stat['empty_rate']}%</td><td>{status}</td></tr>
                    """
                html_content += "</table></div>"

            # 신규 빈 값
            newly_empty = comparison.get('newly_empty', [])
            if newly_empty:
                html_content += f"""
                <div class="section">
                    <h3 class="critical">신규 빈 값 ({len(newly_empty)}건) - 직전 세션에는 값이 있었으나 금회 빈 값으로 변경</h3>
                    <table><tr><th>#</th><th>제품명</th><th>변동 필드</th><th>URL</th></tr>
                """
                for i, rec in enumerate(newly_empty[:30], 1):
                    html_content += f"<tr><td>{i}</td><td>{rec['product_name']}</td><td>{rec['field']}</td><td><a href=\"{rec['url']}\">{rec['url'][-40:]}</a></td></tr>"
                if len(newly_empty) > 30:
                    html_content += f"<tr><td colspan='4'>... 외 {len(newly_empty)-30}건</td></tr>"
                html_content += "</table></div>"

            # 값 복구
            recovered = comparison.get('recovered', [])
            if recovered:
                html_content += f"""
                <div class="section">
                    <h3 class="success">값 복구 ({len(recovered)}건) - 직전 세션에는 빈 값이었으나 금회 값이 채워짐</h3>
                    <table><tr><th>#</th><th>제품명</th><th>변동 필드</th><th>URL</th></tr>
                """
                for i, rec in enumerate(recovered[:30], 1):
                    html_content += f"<tr><td>{i}</td><td>{rec['product_name']}</td><td>{rec['field']}</td><td><a href=\"{rec['url']}\">{rec['url'][-40:]}</a></td></tr>"
                if len(recovered) > 30:
                    html_content += f"<tr><td colspan='4'>... 외 {len(recovered)-30}건</td></tr>"
                html_content += "</table></div>"

            if not newly_empty:
                html_content += '<div class="section"><h3 class="success">신규 빈 값 없음</h3></div>'
            if not recovered:
                html_content += '<div class="section"><h3 class="success">값 복구 없음</h3></div>'

        html_content += """
            <div class="section">
                <p style="color: #666; font-size: 12px;">이 메일은 자동 발송되었습니다.</p>
            </div>
        </body>
        </html>
        """

        # 이메일 발송
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = EMAIL_CONFIG['receiver_email']
        msg.attach(MIMEText(html_content, 'html'))

        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            server.sendmail(
                EMAIL_CONFIG['sender_email'],
                EMAIL_CONFIG['receiver_email'],
                msg.as_string()
            )

        logger.info(f"{retailer} TV 크롤링 리포트 발송: {subject}")
        return True

    except Exception as e:
        logger.error(f"{retailer} TV 크롤링 리포트 발송 실패: {e}")
        return False
