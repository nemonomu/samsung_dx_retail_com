import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from lxml import html

def test_review_page_access():
    """Test accessing review page without login"""

    # Setup Chrome WebDriver
    chrome_options = Options()
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # Anti-detection
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        '''
    })

    try:
        # Test product URL
        test_url = "https://www.amazon.com/INSIGNIA-50-inch-4K-UHD-Fire-TV/dp/B0F19KLHG3"

        print("=" * 80)
        print("Amazon Review Page Access Test")
        print("=" * 80)

        print(f"\n[1] Accessing product page...")
        print(f"    URL: {test_url}")
        driver.get(test_url)
        time.sleep(random.uniform(3, 5))

        print(f"    Current URL: {driver.current_url}")
        print(f"    Title: {driver.title[:60]}...")

        # Extract "See more reviews" link
        print(f"\n[2] Extracting 'See more reviews' link...")
        tree = html.fromstring(driver.page_source)

        # Try multiple XPath patterns
        xpaths = [
            '//*[@id="reviews-medley-footer"]/div[2]/a/@href',
            '//a[@data-hook="see-all-reviews-link-foot"]/@href',
            '//a[contains(text(), "See more reviews")]/@href'
        ]

        review_link = None
        for idx, xpath in enumerate(xpaths, 1):
            result = tree.xpath(xpath)
            if result:
                review_link = result[0]
                print(f"    ✓ Found using XPath #{idx}: {xpath}")
                print(f"    Link: {review_link}")
                break
            else:
                print(f"    ✗ Not found using XPath #{idx}")

        if not review_link:
            print("\n[ERROR] Could not find 'See more reviews' link!")

            # Save page source for debugging
            with open('review_test_page_source.html', 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print("    Saved page source to 'review_test_page_source.html' for debugging")
            return

        # Construct full URL
        if review_link.startswith('http'):
            full_review_url = review_link
        else:
            full_review_url = "https://www.amazon.com" + review_link

        print(f"\n[3] Accessing review page directly...")
        print(f"    URL: {full_review_url}")
        driver.get(full_review_url)
        time.sleep(random.uniform(3, 5))

        print(f"    Current URL: {driver.current_url}")
        print(f"    Title: {driver.title[:60]}...")

        # Check if login is required
        print(f"\n[4] Checking access status...")
        if "sign-in" in driver.current_url.lower() or "ap/signin" in driver.current_url:
            print("    ✗ LOGIN REQUIRED - Redirected to sign-in page")
            print("    Need to implement login or use alternative method")
        else:
            print("    ✓ SUCCESS - Accessed without login!")

            # Try to extract reviews
            print(f"\n[5] Extracting reviews from page...")
            tree = html.fromstring(driver.page_source)

            # Try to find review elements
            review_xpaths = [
                '//div[@data-hook="review"]//span[@data-hook="review-body"]/span/text()',
                '//div[@data-hook="review-collapsed"]//span/text()',
                '//div[contains(@class, "review-text")]//span/text()'
            ]

            reviews_found = []
            for idx, xpath in enumerate(review_xpaths, 1):
                reviews = tree.xpath(xpath)
                if reviews:
                    reviews_found = [r.strip() for r in reviews if r.strip()]
                    print(f"    ✓ Found {len(reviews_found)} reviews using XPath #{idx}")
                    print(f"\n    Sample reviews:")
                    for i, review in enumerate(reviews_found[:3], 1):
                        print(f"      [{i}] {review[:100]}...")
                    break
                else:
                    print(f"    ✗ No reviews found using XPath #{idx}")

            if not reviews_found:
                print("\n    [WARNING] Could not extract reviews")
                # Save page source for debugging
                with open('review_page_source.html', 'w', encoding='utf-8') as f:
                    f.write(driver.page_source)
                print("    Saved page source to 'review_page_source.html' for debugging")

        print("\n" + "=" * 80)
        print("Test completed!")
        print("=" * 80)

    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()

    finally:
        input("\nPress Enter to close browser...")
        driver.quit()

if __name__ == "__main__":
    test_review_page_access()
