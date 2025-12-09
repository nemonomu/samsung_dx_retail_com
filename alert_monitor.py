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

    # Analyze empty value ratio for each field
    fields_to_check = ['final_sku_price', 'count_of_reviews', 'count_of_star_ratings', 'star_rating', 'retailer_sku_name', 'screen_size']
    field_names = {
        'final_sku_price': 'Final SKU Price',
        'count_of_reviews': 'Count of Reviews',
        'count_of_star_ratings': 'Count of Star Ratings',
        'star_rating': 'Star Rating',
        'retailer_sku_name': 'Retailer SKU Name',
        'screen_size': 'Screen Size'
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
        if analysis['is_critical'] or error_message:
            subject = f"[CRITICAL] {retailer_name} TV Crawling Alert - {now.strftime('%Y-%m-%d %H:%M')}"
        elif analysis['alerts']:
            subject = f"[WARNING] {retailer_name} TV Crawling Alert - {now.strftime('%Y-%m-%d %H:%M')}"
        else:
            subject = f"[OK] {retailer_name} TV Crawling Report - {now.strftime('%Y-%m-%d %H:%M')}"

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


def monitor_and_alert(retailer_code, target_count, results_df, error_message=None):
    """
    Monitor crawling results and send alerts (main function)

    Call this function from each crawler.

    Args:
        retailer_code: Retailer code
        target_count: Total tracking list count
        results_df: Crawling results DataFrame (None if failed)
        error_message: Additional error message (optional)

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
