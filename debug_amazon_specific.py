"""
Debug specific Amazon URL to find why wrong message is returned
"""
import time
from lxml import html
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def extract_text_safe(tree, xpath):
    if not xpath:
        return None
    try:
        elements = tree.xpath(xpath)
        if elements:
            if isinstance(elements[0], str):
                return elements[0].strip()
            else:
                return elements[0].text_content().strip()
        return None
    except Exception as e:
        return None

chrome_options = Options()
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=chrome_options
)

try:
    url = "https://www.amazon.com/75-UHD-Hisense-Roku-HDR/dp/B09JVHWJS4/ref=sr_1_302?crid=RY3XOG3VC795&dib=eyJ2IjoiMSJ9.p0XO6n7rnSEC60H0oDaPZA_SCeNY16VZsysust8QGkq1gkMKVytQ8Ykq9jXtxkiGfyRgEZ27f-WOQbPavKaOoEJH5BLIUPJGU7meN-F0rn3o0iFpW2sN0ggBkxO6QPpoyghrZZ12Nrc2aIL2MFkDqh2u4Pw4bLJVq286RGYH49aLcJWrpRhM9s3e97rV8SywgkB6vAIyIvkn2PlLNP5FFeR-P_UdIx3-ecEmKZPcpjQ.fssstYNAbTks4e8c4EuoQ9cEB8v7Mnwh-41hNxXT9A8&dib_tag=se&keywords=TV&qid=1763427228&sprefix=tv%2Caps%2C287&sr=8-302&xpid=Cg0_BApgal-UM"

    print(f"Loading: {url}")
    driver.get(url)
    time.sleep(5)

    page_source = driver.page_source
    tree = html.fromstring(page_source)

    print("=" * 100)
    print("CHECKING SPECIFIC XPATH: #fod-cx-message-with-learn-more")
    print("=" * 100)

    # Check the exact element that both priorities target
    xpath1 = '//*[@id="fod-cx-message-with-learn-more"]/span[1]'
    xpath2 = '//span[@id="fod-cx-message-with-learn-more"]/span[1]'

    text1 = extract_text_safe(tree, xpath1)
    text2 = extract_text_safe(tree, xpath2)

    print(f"XPath 1: {xpath1}")
    print(f"  Text: '{text1}'")
    print(f"  Contains 'Price higher': {'price higher' in text1.lower() if text1 else False}")
    print(f"  Contains 'No featured offers': {'no featured offers' in text1.lower() if text1 else False}")
    print()

    print(f"XPath 2: {xpath2}")
    print(f"  Text: '{text2}'")
    print(f"  Contains 'Price higher': {'price higher' in text2.lower() if text2 else False}")
    print(f"  Contains 'No featured offers': {'no featured offers' in text2.lower() if text2 else False}")
    print()

    print("=" * 100)
    print("CHECKING BROAD XPATHS (contains text)")
    print("=" * 100)

    # Check the broad XPaths
    broad_xpaths = [
        ('//span[contains(text(), "Price higher than typical")]', 'Price higher'),
        ('//span[contains(text(), "No featured offers available")]', 'No featured offers')
    ]

    for xpath, desc in broad_xpaths:
        elements = tree.xpath(xpath)
        print(f"\n{desc}: {xpath}")
        print(f"  Found {len(elements)} element(s)")
        if elements:
            for i, elem in enumerate(elements[:3], 1):  # Show first 3
                text = elem.text_content().strip()
                print(f"  [{i}] '{text[:100]}'")

    print("\n" + "=" * 100)
    print("CHECKING ALL SPANS WITH THESE MESSAGES")
    print("=" * 100)

    # Find ALL spans and check which contain our target messages
    all_spans = tree.xpath('//span')
    price_higher_spans = []
    no_offers_spans = []

    for span in all_spans:
        text = span.text_content().strip()
        if 'price higher than typical' in text.lower():
            price_higher_spans.append(text)
        if 'no featured offers available' in text.lower():
            no_offers_spans.append(text)

    print(f"Spans with 'Price higher than typical': {len(price_higher_spans)}")
    for i, text in enumerate(price_higher_spans, 1):
        print(f"  [{i}] '{text[:100]}'")

    print(f"\nSpans with 'No featured offers available': {len(no_offers_spans)}")
    for i, text in enumerate(no_offers_spans, 1):
        print(f"  [{i}] '{text[:100]}'")

    # Save full page HTML for inspection
    print("\n" + "=" * 100)
    print("SAVING PAGE HTML")
    print("=" * 100)
    with open('amazon_page_source.html', 'w', encoding='utf-8') as f:
        f.write(page_source)
    print("Saved to: amazon_page_source.html")

    # Also check the specific element's HTML
    fod_element = tree.xpath('//*[@id="fod-cx-message-with-learn-more"]')
    if fod_element:
        print("\nElement #fod-cx-message-with-learn-more HTML:")
        print(html.tostring(fod_element[0], encoding='unicode', pretty_print=True)[:500])

finally:
    driver.quit()
    print("\n[DONE]")
