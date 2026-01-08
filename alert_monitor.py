"""
TV Crawling Monitoring and Alert Module
- Crawling result analysis
- Email alert when issues detected
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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
        'shipping_availability', 'delivery_availability', 'shipping_info',
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
        'shipping_availability': 'Shipping Availability',
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
                      fsp_null_records=None, cosr_null_records=None):
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

        # Always send email (daily report)
        return send_alert_email(analysis, error_message)

    except Exception as e:
        logger.error(f"Monitoring error: {e}")
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
