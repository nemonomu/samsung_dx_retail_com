"""
BestBuy Crawler Utility Functions
- URL에서 item 추출
- is_product=false인 item 목록 로드
"""
import psycopg2
from config import DB_CONFIG


def extract_item_from_url(url):
    """
    BestBuy 제품 URL에서 item ID 추출

    Examples:
        https://www.bestbuy.com/product/roku-32-class.../J3PFCJQRY8/sku/6644457 -> J3PFCJQRY8
        https://www.bestbuy.com/product/tcl-75-class.../J36QYTQ595 -> J36QYTQ595

    Args:
        url: BestBuy 제품 URL

    Returns:
        str: item ID or None if extraction fails
    """
    try:
        if not url:
            return None

        # Split URL by "/"
        parts = url.rstrip('/').split('/')

        # Find "product" in the URL
        if 'product' in parts:
            product_index = parts.index('product')
            # Item ID is 2 positions after "product" (product_index + 2)
            if len(parts) > product_index + 2:
                item = parts[product_index + 2]
                # 'sku'가 아닌 경우에만 반환
                if item and item.lower() != 'sku':
                    return item

        return None

    except Exception as e:
        return None


def load_excluded_items(table_name='tv_item_mst'):
    """
    tv_item_mst 테이블에서 is_product=false인 item 목록 로드

    Args:
        table_name: 테이블명 (기본값: tv_item_mst)

    Returns:
        set: is_product=false인 item ID들의 집합
    """
    excluded_items = set()

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT item
            FROM {table_name}
            WHERE is_product = FALSE
        """)

        rows = cursor.fetchall()
        excluded_items = {row[0] for row in rows if row[0]}

        cursor.close()
        conn.close()

        if excluded_items:
            print(f"[CONFIG] Loaded {len(excluded_items)} excluded items (is_product=false)")

    except Exception as e:
        print("[CONFIG] Excluded item list unavailable; continuing without exclusions")

    return excluded_items


def is_excluded_url(url, excluded_items):
    """
    URL이 제외 대상인지 확인

    Args:
        url: 제품 URL
        excluded_items: is_product=false인 item 집합

    Returns:
        bool: True if URL should be excluded
    """
    if not excluded_items:
        return False

    item = extract_item_from_url(url)
    if item and item in excluded_items:
        return True

    return False


# 테스트
if __name__ == '__main__':
    # URL에서 item 추출 테스트
    test_urls = [
        'https://www.bestbuy.com/product/roku-32-class/J3PFCJQRY8/sku/6644457',
        'https://www.bestbuy.com/product/tcl-75-class/J36QYTQ595',
        'https://www.bestbuy.com/product/samsung-55-class/ABC123XYZ',
    ]

    print("=== URL Item Extraction Test ===")
    for url in test_urls:
        item = extract_item_from_url(url)
        print(f"  {url[:60]}... -> {item}")

    print("\n=== Excluded Items Test ===")
    excluded = load_excluded_items()
    print(f"  Excluded items count: {len(excluded)}")
    if excluded:
        print(f"  Sample items: {list(excluded)[:5]}")
