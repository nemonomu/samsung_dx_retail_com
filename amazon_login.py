import time
import pickle
import os
import sys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from config import AMAZON_ACCOUNTS

# Default cookie file path (for backward compatibility)
COOKIE_FILE = 'amazon_cookies.pkl'

# Amazon credentials (will be overridden by account selection)
AMAZON_EMAIL = 'your-email@example.com'
AMAZON_PASSWORD = 'your-password'

def setup_driver():
    """Setup Chrome WebDriver"""
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

    return driver

def save_cookies(driver, filepath):
    """Save cookies to file"""
    with open(filepath, 'wb') as f:
        pickle.dump(driver.get_cookies(), f)
    print(f"[OK] Cookies saved to {filepath}")

def load_cookies(driver, filepath):
    """Load cookies from file"""
    if not os.path.exists(filepath):
        return False

    with open(filepath, 'rb') as f:
        cookies = pickle.load(f)
        for cookie in cookies:
            driver.add_cookie(cookie)
    print(f"[OK] Cookies loaded from {filepath}")
    return True

def login_to_amazon(driver, email, password):
    """Login to Amazon"""
    try:
        print("\n" + "=" * 80)
        print("Amazon Login Process")
        print("=" * 80)

        # Check if already on login page
        current_url = driver.current_url
        already_on_login_page = '/ap/signin' in current_url

        if already_on_login_page:
            print("\n[1] Already on login page, proceeding with login...")
        else:
            # Go to Amazon
            print("\n[1] Accessing Amazon.com...")
            driver.get("https://www.amazon.com")
            time.sleep(3)

            # Check if already logged in
            print("\n[2] Checking if already logged in...")
            try:
                account_element = driver.find_element(By.ID, "nav-link-accountList")
                account_text = account_element.text.lower()
                if "hello" in account_text and "sign in" not in account_text:
                    print("    ✓ Already logged in!")
                    return True
            except:
                pass

            # Click "Sign in" button
            print("\n[3] Clicking 'Sign in' button...")
            try:
                # Try multiple selectors
                sign_in_selectors = [
                    (By.ID, "nav-link-accountList"),
                    (By.CSS_SELECTOR, "a[data-nav-role='signin']"),
                    (By.XPATH, "//a[contains(@href, 'ap/signin')]")
                ]

                signed_in = False
                for by, selector in sign_in_selectors:
                    try:
                        sign_in = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((by, selector))
                        )
                        sign_in.click()
                        signed_in = True
                        print("    ✓ Clicked sign in button")
                        break
                    except:
                        continue

                if not signed_in:
                    print("    [WARNING] Could not find sign-in button, might already be on sign-in page")

                time.sleep(2)

            except Exception as e:
                print(f"    [WARNING] Error clicking sign-in: {e}")

            # Check if redirected to account page (already logged in)
            current_url = driver.current_url
            if '/gp/css/homepage' in current_url or '/gp/yourstore' in current_url:
                print("    ✓ Redirected to account page - already logged in!")
                return True

        # Check for account selection screen first
        step_num = "[2]" if already_on_login_page else "[4]"
        print(f"\n{step_num} Checking for account selection screen...")
        print(f"    Current URL: {driver.current_url}")

        # Save page source for debugging
        with open('login_page_debug.html', 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        print("    [DEBUG] Saved page source to login_page_debug.html")

        # Try to find existing account button (e.g., "lch6322@gmail.com")
        # Multiple selector strategies for different account selection screens
        account_button_selectors = [
            # Strategy 1: Account selection dropdown
            (By.CSS_SELECTOR, "div[data-a-input-name='accountSelectionSelect'] span.a-button-text"),
            (By.XPATH, "//div[@data-a-input-name='accountSelectionSelect']//span[contains(@class, 'a-button-text')]"),

            # Strategy 2: "Switch accounts" section - clickable account div/span
            (By.XPATH, "//div[contains(@class, 'cvf-account-switcher-account')]"),
            (By.XPATH, "//div[contains(@data-testid, 'account-list-item')]"),
            (By.CSS_SELECTOR, "div[data-testid*='account-list-item']"),

            # Strategy 3: Find by email text (common pattern)
            (By.XPATH, f"//span[contains(text(), '{email}')]"),
            (By.XPATH, f"//div[contains(text(), '{email}')]"),

            # Strategy 4: Generic - any element with @ symbol (email)
            (By.XPATH, "//span[contains(text(), '@')]"),
            (By.XPATH, "//div[contains(text(), '@')]"),

            # Strategy 5: Account card/container
            (By.CSS_SELECTOR, "div.cvf-account-switcher-account"),
            (By.CSS_SELECTOR, "div[class*='account']"),
        ]

        account_found = False
        for idx, (by, selector) in enumerate(account_button_selectors, 1):
            try:
                account_button = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((by, selector))
                )
                print(f"    ✓ Found existing account button using selector #{idx}")
                button_text = account_button.text[:80] if account_button.text else "[No text]"
                print(f"    ✓ Clicking account: {button_text}...")
                account_button.click()
                account_found = True
                time.sleep(2)
                break
            except:
                continue

        if account_found:
            print("    ✓ Selected existing account, skipping email entry")
        else:
            # No account selection, proceed with email entry
            print("    ✗ No account selection screen, entering email...")

            # Try multiple selectors for email field
            email_selectors = [
                (By.ID, "ap_email"),
                (By.NAME, "email"),
                (By.CSS_SELECTOR, "input[type='email']"),
                (By.CSS_SELECTOR, "input[name='email']"),
                (By.XPATH, "//input[@type='email']")
            ]

            email_input = None
            for idx, (by, selector) in enumerate(email_selectors, 1):
                try:
                    email_input = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((by, selector))
                    )
                    print(f"    ✓ Found email input using selector #{idx}: {selector}")
                    break
                except:
                    print(f"    ✗ Not found using selector #{idx}: {selector}")
                    continue

            if not email_input:
                print("\n[ERROR] Could not find email input field!")
                print("Please check login_page_debug.html for page structure")
                raise Exception("Email input field not found")

            email_input.clear()
            email_input.send_keys(email)
            print("    ✓ Email entered")
            time.sleep(1)

            # Click Continue
            print("\n[4] Clicking 'Continue' button...")
            continue_selectors = [
                (By.ID, "continue"),
                (By.CSS_SELECTOR, "input[type='submit']"),
                (By.CSS_SELECTOR, "input.a-button-input"),
                (By.XPATH, "//input[@id='continue']")
            ]

            for idx, (by, selector) in enumerate(continue_selectors, 1):
                try:
                    continue_button = driver.find_element(by, selector)
                    continue_button.click()
                    print(f"    ✓ Clicked continue button using selector #{idx}")
                    break
                except:
                    continue

            time.sleep(3)

        # Enter password
        print("\n[5] Entering password...")
        password_selectors = [
            (By.ID, "ap_password"),
            (By.NAME, "password"),
            (By.CSS_SELECTOR, "input[type='password']"),
            (By.CSS_SELECTOR, "input[name='password']"),
            (By.XPATH, "//input[@type='password']")
        ]

        password_input = None
        for idx, (by, selector) in enumerate(password_selectors, 1):
            try:
                password_input = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((by, selector))
                )
                print(f"    ✓ Found password input using selector #{idx}: {selector}")
                break
            except:
                print(f"    ✗ Not found using selector #{idx}: {selector}")
                continue

        if not password_input:
            print("\n[ERROR] Could not find password input field!")
            raise Exception("Password input field not found")

        password_input.clear()
        password_input.send_keys(password)
        print("    ✓ Password entered")
        time.sleep(1)

        # Click Sign-In
        print("\n[6] Clicking 'Sign-In' button...")
        signin_selectors = [
            (By.ID, "signInSubmit"),
            (By.CSS_SELECTOR, "input[type='submit']"),
            (By.CSS_SELECTOR, "input.a-button-input"),
            (By.XPATH, "//input[@id='signInSubmit']")
        ]

        for idx, (by, selector) in enumerate(signin_selectors, 1):
            try:
                sign_in_button = driver.find_element(by, selector)
                sign_in_button.click()
                print(f"    ✓ Clicked sign-in button using selector #{idx}")
                break
            except:
                continue

        time.sleep(5)

        # Check for CAPTCHA or OTP
        print("\n[7] Checking for CAPTCHA or OTP...")
        current_url = driver.current_url

        if "ap/cvf" in current_url or "ap/mfa" in current_url:
            print("\n" + "!" * 80)
            print("! OTP / 2-Factor Authentication Required!")
            print("! Please enter the OTP code manually in the browser")
            print("! Waiting 60 seconds for you to complete...")
            print("!" * 80)
            time.sleep(60)

        elif "ap/captcha" in current_url or "captcha" in driver.page_source.lower():
            print("\n" + "!" * 80)
            print("! CAPTCHA Detected!")
            print("! Please solve the CAPTCHA manually in the browser")
            print("! Waiting 60 seconds for you to complete...")
            print("!" * 80)
            time.sleep(60)

        # Verify login success
        print("\n[8] Verifying login status...")
        driver.get("https://www.amazon.com")
        time.sleep(3)

        # Check if logged in
        try:
            account_element = driver.find_element(By.ID, "nav-link-accountList")
            account_text = account_element.text.lower()

            if "hello" in account_text and "sign in" not in account_text:
                print("\n" + "=" * 80)
                print("✓ LOGIN SUCCESSFUL!")
                print("=" * 80)
                return True
            else:
                print("\n" + "=" * 80)
                print("✗ LOGIN FAILED - Still showing 'Sign in'")
                print("=" * 80)
                return False

        except Exception as e:
            print(f"\n[WARNING] Could not verify login status: {e}")
            print("Assuming login successful...")
            return True

    except Exception as e:
        print(f"\n[ERROR] Login failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_login_with_cookies():
    """Test login using saved cookies or fresh login"""
    driver = setup_driver()

    try:
        # Try to load cookies first
        if os.path.exists(COOKIE_FILE):
            print(f"\n[INFO] Found saved cookies: {COOKIE_FILE}")
            print("[INFO] Trying to login with saved cookies...")

            driver.get("https://www.amazon.com")
            time.sleep(2)

            load_cookies(driver, COOKIE_FILE)
            driver.refresh()
            time.sleep(3)

            # Check if logged in
            try:
                account_element = driver.find_element(By.ID, "nav-link-accountList")
                account_text = account_element.text.lower()

                if "hello" in account_text and "sign in" not in account_text:
                    print("\n✓ Successfully logged in using saved cookies!")
                    return driver
                else:
                    print("\n✗ Saved cookies expired, need fresh login")
            except:
                print("\n✗ Could not verify cookie login, need fresh login")

        # Fresh login
        print("\n[INFO] Starting fresh login...")
        print(f"[INFO] Email: {AMAZON_EMAIL}")
        print(f"[INFO] Password: {'*' * len(AMAZON_PASSWORD)}")

        if AMAZON_EMAIL == 'your-email@example.com':
            print("\n" + "!" * 80)
            print("! ERROR: Please edit amazon_login.py and set your Amazon credentials!")
            print("! Change AMAZON_EMAIL and AMAZON_PASSWORD at the top of the file")
            print("!" * 80)
            return None

        if login_to_amazon(driver, AMAZON_EMAIL, AMAZON_PASSWORD):
            save_cookies(driver, COOKIE_FILE)
            return driver
        else:
            print("\n[ERROR] Login failed!")
            return None

    except Exception as e:
        print(f"\n[ERROR] Test failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def login_with_account(account_name):
    """Login with specific account from config"""
    if account_name not in AMAZON_ACCOUNTS:
        print(f"\n[ERROR] Account '{account_name}' not found in config.py")
        print(f"Available accounts: {', '.join(AMAZON_ACCOUNTS.keys())}")
        return None

    account = AMAZON_ACCOUNTS[account_name]
    email = account['email']
    password = account['password']
    cookie_file = account['cookie_file']

    print(f"\n{'='*80}")
    print(f"Amazon Login - Account: {account_name}")
    print(f"Email: {email}")
    print(f"Cookie file: {cookie_file}")
    print(f"{'='*80}")

    driver = setup_driver()

    try:
        # Try to load existing cookies first
        if os.path.exists(cookie_file):
            print(f"\n[INFO] Found existing cookies: {cookie_file}")
            print("[INFO] Trying to login with saved cookies...")

            driver.get("https://www.amazon.com")
            time.sleep(2)

            load_cookies(driver, cookie_file)
            driver.refresh()
            time.sleep(3)

            # Check if logged in
            try:
                account_element = driver.find_element(By.ID, "nav-link-accountList")
                account_text = account_element.text.lower()

                if "hello" in account_text and "sign in" not in account_text:
                    print("\n✓ Successfully logged in using saved cookies!")
                    return driver
                else:
                    print("\n✗ Saved cookies expired, need fresh login")
            except:
                print("\n✗ Could not verify cookie login, need fresh login")

        # Fresh login
        print("\n[INFO] Starting fresh login...")

        if login_to_amazon(driver, email, password):
            save_cookies(driver, cookie_file)
            print(f"\n[OK] Cookies saved to {cookie_file}")
            return driver
        else:
            print("\n[ERROR] Login failed!")
            return None

    except Exception as e:
        print(f"\n[ERROR] Login failed: {e}")
        import traceback
        traceback.print_exc()
        return None


# ============================================================
# DrissionPage compatible functions (for amazon_tv_dt1/dt2)
# ============================================================

def save_cookies_dp(page, filepath):
    """Save cookies to file (DrissionPage version)"""
    try:
        cookies = page.cookies(as_dict=False)
        with open(filepath, 'wb') as f:
            pickle.dump(cookies, f)
        print(f"[OK] Cookies saved to {filepath}")
    except Exception as e:
        print(f"[WARNING] Failed to save cookies: {e}")


def login_to_amazon_dp(page, email, password):
    """Login to Amazon (DrissionPage version)"""
    try:
        print("\n" + "=" * 80)
        print("Amazon Login Process (DrissionPage)")
        print("=" * 80)

        # Check if already on login page
        current_url = page.url
        already_on_login_page = '/ap/signin' in current_url

        if already_on_login_page:
            print("\n[1] Already on login page, proceeding with login...")
        else:
            # Go to Amazon
            print("\n[1] Accessing Amazon.com...")
            page.get("https://www.amazon.com")
            time.sleep(3)

            # Check if already logged in
            print("\n[2] Checking if already logged in...")
            try:
                account_element = page.ele('#nav-link-accountList', timeout=5)
                if account_element:
                    account_text = account_element.text.lower()
                    if "hello" in account_text and "sign in" not in account_text:
                        print("    ✓ Already logged in!")
                        return True
            except:
                pass

            # Click "Sign in" button
            print("\n[3] Clicking 'Sign in' button...")
            try:
                sign_in_selectors = [
                    '#nav-link-accountList',
                    'css:a[data-nav-role="signin"]',
                    'xpath://a[contains(@href, "ap/signin")]'
                ]

                signed_in = False
                for selector in sign_in_selectors:
                    try:
                        sign_in = page.ele(selector, timeout=5)
                        if sign_in:
                            sign_in.click()
                            signed_in = True
                            print("    ✓ Clicked sign in button")
                            break
                    except:
                        continue

                if not signed_in:
                    print("    [WARNING] Could not find sign-in button, might already be on sign-in page")

                time.sleep(2)

            except Exception as e:
                print(f"    [WARNING] Error clicking sign-in: {e}")

            # Check if redirected to account page (already logged in)
            current_url = page.url
            if '/gp/css/homepage' in current_url or '/gp/yourstore' in current_url:
                print("    ✓ Redirected to account page - already logged in!")
                return True

        # Check for account selection screen first
        step_num = "[2]" if already_on_login_page else "[4]"
        print(f"\n{step_num} Checking for account selection screen...")
        print(f"    Current URL: {page.url}")

        # Save page source for debugging
        try:
            with open('login_page_debug.html', 'w', encoding='utf-8') as f:
                f.write(page.html)
            print("    [DEBUG] Saved page source to login_page_debug.html")
        except:
            pass

        # Try to find existing account button
        account_button_selectors = [
            'css:div[data-a-input-name="accountSelectionSelect"] span.a-button-text',
            'xpath://div[@data-a-input-name="accountSelectionSelect"]//span[contains(@class, "a-button-text")]',
            'xpath://div[contains(@class, "cvf-account-switcher-account")]',
            'xpath://div[contains(@data-testid, "account-list-item")]',
            'css:div[data-testid*="account-list-item"]',
            f'xpath://span[contains(text(), "{email}")]',
            f'xpath://div[contains(text(), "{email}")]',
            'xpath://span[contains(text(), "@")]',
            'xpath://div[contains(text(), "@")]',
            'css:div.cvf-account-switcher-account',
            'css:div[class*="account"]',
        ]

        account_found = False
        for idx, selector in enumerate(account_button_selectors, 1):
            try:
                account_button = page.ele(selector, timeout=3)
                if account_button:
                    print(f"    ✓ Found existing account button using selector #{idx}")
                    button_text = account_button.text[:80] if account_button.text else "[No text]"
                    print(f"    ✓ Clicking account: {button_text}...")
                    account_button.click()
                    account_found = True
                    time.sleep(2)
                    break
            except:
                continue

        if account_found:
            print("    ✓ Selected existing account, skipping email entry")
        else:
            # No account selection, proceed with email entry
            print("    ✗ No account selection screen, entering email...")

            email_selectors = [
                '#ap_email',
                'css:input[name="email"]',
                'css:input[type="email"]',
                'xpath://input[@type="email"]'
            ]

            email_input = None
            for idx, selector in enumerate(email_selectors, 1):
                try:
                    email_input = page.ele(selector, timeout=5)
                    if email_input:
                        print(f"    ✓ Found email input using selector #{idx}")
                        break
                except:
                    print(f"    ✗ Not found using selector #{idx}")
                    continue

            if not email_input:
                print("\n[ERROR] Could not find email input field!")
                print("Please check login_page_debug.html for page structure")
                raise Exception("Email input field not found")

            email_input.clear()
            email_input.input(email)
            print("    ✓ Email entered")
            time.sleep(1)

            # Click Continue
            print("\n[4] Clicking 'Continue' button...")
            continue_selectors = [
                '#continue',
                'css:input[type="submit"]',
                'css:input.a-button-input',
                'xpath://input[@id="continue"]'
            ]

            for idx, selector in enumerate(continue_selectors, 1):
                try:
                    continue_button = page.ele(selector, timeout=3)
                    if continue_button:
                        continue_button.click()
                        print(f"    ✓ Clicked continue button using selector #{idx}")
                        break
                except:
                    continue

            time.sleep(3)

        # Enter password
        print("\n[5] Entering password...")
        password_selectors = [
            '#ap_password',
            'css:input[name="password"]',
            'css:input[type="password"]',
            'xpath://input[@type="password"]'
        ]

        password_input = None
        for idx, selector in enumerate(password_selectors, 1):
            try:
                password_input = page.ele(selector, timeout=5)
                if password_input:
                    print(f"    ✓ Found password input using selector #{idx}")
                    break
            except:
                print(f"    ✗ Not found using selector #{idx}")
                continue

        if not password_input:
            print("\n[ERROR] Could not find password input field!")
            raise Exception("Password input field not found")

        password_input.clear()
        password_input.input(password)
        print("    ✓ Password entered")
        time.sleep(1)

        # Click Sign-In
        print("\n[6] Clicking 'Sign-In' button...")
        signin_selectors = [
            '#signInSubmit',
            'css:input[type="submit"]',
            'css:input.a-button-input',
            'xpath://input[@id="signInSubmit"]'
        ]

        for idx, selector in enumerate(signin_selectors, 1):
            try:
                sign_in_button = page.ele(selector, timeout=3)
                if sign_in_button:
                    sign_in_button.click()
                    print(f"    ✓ Clicked sign-in button using selector #{idx}")
                    break
            except:
                continue

        time.sleep(5)

        # Check for CAPTCHA or OTP
        print("\n[7] Checking for CAPTCHA or OTP...")
        current_url = page.url

        if "ap/cvf" in current_url or "ap/mfa" in current_url:
            print("\n" + "!" * 80)
            print("! OTP / 2-Factor Authentication Required!")
            print("! Please enter the OTP code manually in the browser")
            print("! Waiting 60 seconds for you to complete...")
            print("!" * 80)
            time.sleep(60)

        elif "ap/captcha" in current_url or "captcha" in page.html.lower():
            print("\n" + "!" * 80)
            print("! CAPTCHA Detected!")
            print("! Please solve the CAPTCHA manually in the browser")
            print("! Waiting 60 seconds for you to complete...")
            print("!" * 80)
            time.sleep(60)

        # Verify login success
        print("\n[8] Verifying login status...")
        page.get("https://www.amazon.com")
        time.sleep(3)

        # Check if logged in
        try:
            account_element = page.ele('#nav-link-accountList', timeout=5)
            if account_element:
                account_text = account_element.text.lower()
                if "hello" in account_text and "sign in" not in account_text:
                    print("\n" + "=" * 80)
                    print("✓ LOGIN SUCCESSFUL!")
                    print("=" * 80)
                    return True
                else:
                    print("\n" + "=" * 80)
                    print("✗ LOGIN FAILED - Still showing 'Sign in'")
                    print("=" * 80)
                    return False
            else:
                print(f"\n[WARNING] Could not find account element")
                print("Assuming login successful...")
                return True

        except Exception as e:
            print(f"\n[WARNING] Could not verify login status: {e}")
            print("Assuming login successful...")
            return True

    except Exception as e:
        print(f"\n[ERROR] Login failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) > 1:
        account_name = sys.argv[1]
        driver = login_with_account(account_name)
    else:
        # Show available accounts and prompt for selection
        print("\n" + "=" * 80)
        print("Amazon Login - Account Selection")
        print("=" * 80)
        print("\nAvailable accounts:")
        for idx, (name, info) in enumerate(AMAZON_ACCOUNTS.items(), 1):
            print(f"  {idx}. {name} ({info['email']})")
        print(f"  0. Use default (legacy mode)")

        print("\nUsage: python amazon_login.py <account_name>")
        print("Example: python amazon_login.py unsandev0002")
        print("\nOr select account number:")

        try:
            choice = input("Enter choice (0-3): ").strip()
            if choice == '0':
                driver = test_login_with_cookies()
            elif choice.isdigit() and 1 <= int(choice) <= len(AMAZON_ACCOUNTS):
                account_name = list(AMAZON_ACCOUNTS.keys())[int(choice) - 1]
                driver = login_with_account(account_name)
            else:
                print("[ERROR] Invalid choice")
                driver = None
        except KeyboardInterrupt:
            print("\n[INFO] Cancelled by user")
            driver = None

    if driver:
        print("\n" + "=" * 80)
        print("Login completed successfully!")
        print("You can now use the saved cookies for crawling")
        print("=" * 80)

        input("\nPress Enter to close browser...")
        driver.quit()
    else:
        print("\n[FAILED] Login failed")
