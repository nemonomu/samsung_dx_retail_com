"""
Save Walmart session cookies for reuse
Run this locally with headless=False to manually bypass bot detection
"""
import time
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync
import json

def save_cookies():
    """Open browser, let user bypass bot detection, then save cookies"""
    print("="*80)
    print("Walmart Cookie Saver")
    print("="*80)
    print("\n[INFO] This script will:")
    print("  1. Open a browser window")
    print("  2. Navigate to Walmart.com")
    print("  3. Wait for you to manually bypass any bot detection")
    print("  4. Save the cookies to walmart_cookies.json")
    print("\n[ACTION REQUIRED] After Walmart loads successfully, press ENTER in this terminal")
    print("="*80 + "\n")

    with sync_playwright() as p:
        # Launch browser with GUI (headless=False)
        browser = p.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-dev-shm-usage'
            ]
        )

        # Create context with realistic settings
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='America/New_York'
        )

        page = context.new_page()

        # Apply stealth
        stealth_sync(page)

        print("[INFO] Opening Walmart homepage...")
        page.goto("https://www.walmart.com", wait_until='domcontentloaded')

        print("\n[WAITING] Please solve any CAPTCHA or bot detection in the browser window...")
        print("[WAITING] Once Walmart homepage loads successfully, come back here and press ENTER")
        input("\nPress ENTER after you've bypassed bot detection and see the Walmart homepage...")

        print("\n[INFO] Testing if we can access best seller page...")
        page.goto("https://www.walmart.com/search?q=TV&sort=best_seller", wait_until='domcontentloaded')
        time.sleep(5)

        # Check if robot page appears
        content = page.content()
        if "Robot or human?" in content or "Enter the characters you see below" in content:
            print("\n[WARNING] Still showing bot detection page!")
            print("[INFO] Please solve the CAPTCHA again, then press ENTER")
            input("\nPress ENTER after solving CAPTCHA on best seller page...")
        else:
            print("[OK] Best seller page loaded successfully!")

        # Save cookies
        cookies = context.cookies()

        # Save to JSON file
        with open('walmart_cookies.json', 'w') as f:
            json.dump(cookies, f, indent=2)

        print(f"\n[SUCCESS] Saved {len(cookies)} cookies to walmart_cookies.json")

        # Also save storage state (includes localStorage)
        context.storage_state(path='walmart_storage_state.json')
        print(f"[SUCCESS] Saved storage state to walmart_storage_state.json")

        print("\n[INFO] You can now close the browser window")
        print("[INFO] Upload these files to AWS:")
        print("  - walmart_cookies.json")
        print("  - walmart_storage_state.json")
        print("="*80)

        input("\nPress ENTER to close browser...")

        browser.close()

if __name__ == "__main__":
    save_cookies()
