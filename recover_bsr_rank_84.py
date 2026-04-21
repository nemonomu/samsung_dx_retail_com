"""
Unified recovery for ALL missing BSR ranks in batch 20260421_130951 (detail_crawled).

누락 6개:
  amazon_tv_bsr 원본에만 존재 (detail_crawled에 bsr_rank 미반영):
    rank 10 -> URL(B0DFNTKTYN)    [expected HIT → UPDATE]
    rank 27 -> URL(B0DVX8WJ7S)    [expected HIT → UPDATE]
    rank 28 -> URL(B0CJDSNN4T)    [expected MISS → scrape]
    rank 53 -> URL(B0GM3KH77D)    [expected HIT → UPDATE]
    rank 69 -> URL(B0CZBLZYY5 variant → parent B08SVZ775L HIT via DEDUP on scrape]
  amazon_tv_bsr 원본에도 누락:
    rank 84 -> BSR page 2 스크랩으로 URL 획득 후 동일 로직

규칙:
  - crawl_datetime만 세션 범위(2026-04-21 00:11 ~ 02:42) 임의값
  - 나머지 모든 필드는 실수집 or 동일 URL의 기존 세션 데이터 JOIN
  - tv_retail_com 구(舊) row(batch_id=NULL)는 item+crawl_datetime+account_name으로 UPDATE 매칭

사용법:
  python recover_bsr_rank_84.py
"""
import sys
import os
import time
import random
import re
from datetime import datetime
import pytz
import psycopg2
from lxml import html

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import DB_CONFIG
from amazon_tv_dt2 import AmazonDetailCrawler

TARGET_BSR_BATCH = '20260421_130730'   # amazon_tv_bsr 소스 batch
TARGET_DT_BATCH = '20260421_130951'     # amazon_tv_detail_crawled / tv_retail_com 대상 batch
BSR_PAGE_URL_P2 = (
    'https://www.amazon.com/Best-Sellers-Electronics-Televisions/zgbs/electronics/'
    '172659/ref=zg_bs_pg_2_electronics?_encoding=UTF8&pg=2'
)
CALENDAR_WEEK = 'w17'
SESSION_CRAWL_DT = '2026-04-21 01:30:00'

# amazon_tv_bsr에 이미 있는 rank들 (URL만 DB에서 조회)
RANKS_WITH_URL = [10, 27, 28, 53, 69]
# amazon_tv_bsr에도 없는 rank (BSR page 2 스크랩 필요)
RANKS_TO_SCRAPE = [84]
ALL_MISSING_RANKS = RANKS_WITH_URL + RANKS_TO_SCRAPE


def load_bsr_xpaths(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT data_field, xpath FROM public.xpath_selectors
        WHERE mall_name='Amazon' AND page_type='bsr_page' AND is_active=TRUE
    """)
    xp = {r[0]: r[1] for r in cur.fetchall()}
    cur.close()
    return xp


def load_urls_for_ranks_with_url(conn):
    """amazon_tv_bsr에서 해당 rank의 URL/name/reviews/rating 조회"""
    cur = conn.cursor()
    cur.execute("""
        SELECT bsr_rank, product_url, Retailer_SKU_Name, count_of_reviews, star_rating
        FROM public.amazon_tv_bsr
        WHERE batch_id=%s AND bsr_rank = ANY(%s)
        ORDER BY bsr_rank
    """, (TARGET_BSR_BATCH, RANKS_WITH_URL))
    out = {}
    for r in cur.fetchall():
        out[r[0]] = {
            'bsr_rank': r[0],
            'product_url': r[1],
            'product_name': r[2],
            'count_of_reviews': r[3],
            'star_rating': r[4],
        }
    cur.close()
    return out


def scrape_bsr_page2_rank(page, xpaths, target_rank):
    """BSR page 2에서 지정 rank의 container 추출 (bsr1.py 로직 준용)"""
    print(f"\n[STEP] Loading BSR page 2 for rank {target_rank}: {BSR_PAGE_URL_P2[:70]}...")
    page.get(BSR_PAGE_URL_P2)
    time.sleep(random.uniform(8, 12))

    for _ in range(6):
        page.scroll.to_bottom()
        time.sleep(random.uniform(1.5, 2.5))

    tree = html.fromstring(page.html)
    containers = tree.xpath(xpaths['base_container'])
    print(f"[INFO] Found {len(containers)} containers on page 2")

    for c in containers:
        rank_nodes = c.xpath(xpaths['rank'])
        if not rank_nodes:
            continue
        n = rank_nodes[0]
        rank_text = n.text_content().strip() if hasattr(n, 'text_content') else str(n).strip()
        try:
            rank = int(rank_text.replace('#', '').strip())
        except ValueError:
            continue
        if rank != target_rank:
            continue

        name_nodes = c.xpath(xpaths['product_name'])
        product_name = name_nodes[0].text_content().strip() if name_nodes else None

        url_nodes = c.xpath(xpaths['product_url'])
        product_url = None
        if url_nodes:
            raw = url_nodes[0]
            if isinstance(raw, str):
                product_url = raw if raw.startswith('http') else f'https://www.amazon.com{raw}'
            else:
                href = raw.get('href') if hasattr(raw, 'get') else None
                if href:
                    product_url = href if href.startswith('http') else f'https://www.amazon.com{href}'

        cor_xp = xpaths.get('count_of_reviews')
        cor_nodes = c.xpath(cor_xp) if cor_xp else []
        cor = cor_nodes[0].text_content().strip() if cor_nodes else None

        sr_xp = xpaths.get('star_rating')
        sr_nodes = c.xpath(sr_xp) if sr_xp else []
        sr_raw = sr_nodes[0].text_content().strip() if sr_nodes else None
        sr_match = re.search(r'[\d.]+', sr_raw or '')
        star_rating = sr_match.group() if sr_match else None

        return {
            'bsr_rank': rank,
            'product_name': product_name,
            'product_url': product_url,
            'count_of_reviews': cor,
            'star_rating': star_rating,
        }
    return None


def insert_amazon_tv_bsr(conn, data):
    """amazon_tv_bsr INSERT — 세션 시간대 crawl_strdatetime"""
    cur = conn.cursor()
    crawl_strdt = '202604211307' + str(random.randint(3100, 3999)) + str(random.randint(1000, 9999))
    cur.execute("""
        INSERT INTO public.amazon_tv_bsr
        (account_name, bsr_rank, page_type, Retailer_SKU_Name, product_url,
         final_sku_price, original_sku_price, count_of_reviews, star_rating,
         batch_id, calendar_week, crawl_strdatetime)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """, ('Amazon', data['bsr_rank'], 'bsr', data['product_name'], data['product_url'],
          None, None, data['count_of_reviews'], data['star_rating'],
          TARGET_BSR_BATCH, CALENDAR_WEEK, crawl_strdt))
    new_id = cur.fetchone()[0]
    cur.close()
    return new_id


def process_rank(conn, crawler, rank, rank_data):
    """HIT/MISS 분기. rank 84는 호출 전 amazon_tv_bsr INSERT 필요."""
    product_url = rank_data['product_url']
    if not product_url:
        print(f"[ERROR] rank {rank}: no product_url")
        return

    asin = crawler.extract_asin(product_url)
    if not asin or len(asin) != 10:
        print(f"[ERROR] rank {rank}: invalid ASIN '{asin}' from url")
        return

    # HIT 조회
    dcur = conn.cursor()
    dcur.execute("""
        SELECT id, bsr_rank, main_rank, crawl_datetime
        FROM public.amazon_tv_detail_crawled
        WHERE batch_id=%s AND item=%s
    """, (TARGET_DT_BATCH, asin))
    existing = dcur.fetchone()
    dcur.close()

    if existing:
        dc_id, cur_bsr, cur_main, existing_dt = existing
        if cur_bsr is not None:
            print(f"  [HIT-SKIP] rank={rank} item={asin}: bsr_rank already {cur_bsr}")
            return

        print(f"  [HIT] rank={rank} item={asin}: UPDATE (detail id={dc_id}, dt={existing_dt})")
        ucur = conn.cursor()
        ucur.execute("""
            UPDATE public.amazon_tv_detail_crawled SET bsr_rank=%s WHERE id=%s
        """, (rank, dc_id))
        d_rows = ucur.rowcount
        ucur.execute("""
            UPDATE public.tv_retail_com SET bsr_rank=%s
            WHERE item=%s AND crawl_datetime=%s AND account_name='Amazon' AND bsr_rank IS NULL
        """, (rank, asin, existing_dt))
        t_rows = ucur.rowcount
        ucur.close()
        print(f"        -> detail={d_rows}, retail={t_rows}")
        return

    # MISS: 상세 스크랩 (dt2 crawler 사용, crawl_datetime 세션값으로 정규화)
    print(f"  [MISS] rank={rank} item={asin}: fresh scrape")

    # Pre-seed processed_asins: variant→parent redirect 시 dt2 DEDUP UPDATE 작동
    pcur = conn.cursor()
    pcur.execute("""
        SELECT DISTINCT item FROM public.amazon_tv_detail_crawled
        WHERE batch_id=%s AND item IS NOT NULL
    """, (TARGET_DT_BATCH,))
    crawler.processed_asins = set()
    for r in pcur.fetchall():
        crawler.processed_asins.add(r[0])
    pcur.close()

    # pre-count (신규 INSERT 감지용)
    pcur = conn.cursor()
    pcur.execute("SELECT COUNT(*) FROM public.amazon_tv_detail_crawled WHERE batch_id=%s",
                 (TARGET_DT_BATCH,))
    pre_count = pcur.fetchone()[0]
    pcur.close()

    url_data = {
        'asin': asin,
        'page_type': 'bsr',
        'url': product_url,
        'main_rank': None,
        'bsr_rank': rank,
    }
    success = crawler.scrape_detail_page(url_data)
    print(f"       scrape_detail_page -> {success}")

    # post-count
    pcur = conn.cursor()
    pcur.execute("SELECT COUNT(*) FROM public.amazon_tv_detail_crawled WHERE batch_id=%s",
                 (TARGET_DT_BATCH,))
    post_count = pcur.fetchone()[0]
    pcur.close()

    if post_count > pre_count:
        # 신규 INSERT 발생 → crawl_datetime 세션값으로 정규화
        ucur = conn.cursor()
        ucur.execute("""
            UPDATE public.amazon_tv_detail_crawled
            SET crawl_datetime=%s, calendar_week=%s
            WHERE batch_id=%s AND bsr_rank=%s
        """, (SESSION_CRAWL_DT, CALENDAR_WEEK, TARGET_DT_BATCH, rank))
        d_rows = ucur.rowcount
        ucur.execute("""
            UPDATE public.tv_retail_com
            SET crawl_datetime=%s, calendar_week=%s
            WHERE batch_id=%s AND bsr_rank=%s
        """, (SESSION_CRAWL_DT, CALENDAR_WEEK, TARGET_DT_BATCH, rank))
        t_rows = ucur.rowcount
        ucur.close()
        print(f"       NEW row + crawl_datetime normalized: detail={d_rows}, retail={t_rows}")
    else:
        print(f"       DEDUP path taken (existing row updated by dt2 patch)")


def main():
    print(f"=== Unified BSR recovery for detail batch {TARGET_DT_BATCH} ===")
    print(f"    Target missing ranks: {ALL_MISSING_RANKS}\n")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True

    # 사전 상태 조회
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(DISTINCT bsr_rank) FROM public.amazon_tv_detail_crawled
        WHERE batch_id=%s AND bsr_rank IS NOT NULL
    """, (TARGET_DT_BATCH,))
    pre_detail = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.amazon_tv_bsr WHERE batch_id=%s",
                (TARGET_BSR_BATCH,))
    pre_bsr = cur.fetchone()[0]
    print(f"[PRE] detail_crawled distinct bsr_rank: {pre_detail}")
    print(f"[PRE] amazon_tv_bsr rows: {pre_bsr}")
    cur.close()

    bsr_xpaths = load_bsr_xpaths(conn)
    print(f"[OK] Loaded {len(bsr_xpaths)} BSR xpaths")

    urls_from_db = load_urls_for_ranks_with_url(conn)
    print(f"[OK] URLs from amazon_tv_bsr: {len(urls_from_db)} ranks {list(urls_from_db.keys())}")

    # dt2 crawler 초기화 (브라우저 단일 세션 공유)
    crawler = AmazonDetailCrawler()
    crawler.batch_id = TARGET_DT_BATCH
    if not crawler.connect_db():
        return
    if not crawler.load_xpaths():
        return
    crawler.setup_driver()

    try:
        # Phase 1: RANKS_TO_SCRAPE (rank 84) — BSR page 2 스크랩 → INSERT amazon_tv_bsr → process
        scraped_data = {}
        for rank in RANKS_TO_SCRAPE:
            # preflight: 이미 있으면 skip
            pre = conn.cursor()
            pre.execute("SELECT id FROM public.amazon_tv_bsr WHERE batch_id=%s AND bsr_rank=%s",
                        (TARGET_BSR_BATCH, rank))
            exists = pre.fetchone()
            pre.close()
            if exists:
                print(f"\n[SKIP] amazon_tv_bsr already has rank {rank}")
                continue

            data = scrape_bsr_page2_rank(crawler.page, bsr_xpaths, rank)
            if not data or not data.get('product_url'):
                print(f"[ERROR] rank {rank}: not found on BSR page 2")
                continue

            print(f"\n[FOUND] rank {rank}:")
            print(f"  name : {(data['product_name'] or '')[:80]}")
            print(f"  url  : {data['product_url'][:100]}")
            print(f"  star : {data['star_rating']}   reviews: {data['count_of_reviews']}")
            scraped_data[rank] = data

        if scraped_data:
            resp = input(f"\n위 {len(scraped_data)}개 rank을 amazon_tv_bsr에 INSERT할까요? [y/N]: ").strip().lower()
            if resp != 'y':
                print("[CANCEL] scrape 결과 INSERT 취소")
                return
            for rank, data in scraped_data.items():
                new_id = insert_amazon_tv_bsr(conn, data)
                print(f"[OK] amazon_tv_bsr INSERT rank={rank} id={new_id}")

        # Phase 2: 모든 rank에 대해 HIT/MISS 분기 처리
        print(f"\n--- Phase 2: detail_crawled/tv_retail_com 처리 ---")
        all_data = dict(urls_from_db)
        all_data.update(scraped_data)
        for rank in ALL_MISSING_RANKS:
            if rank not in all_data:
                print(f"[SKIP] rank {rank}: no source data")
                continue
            print(f"\n>>> rank {rank}")
            process_rank(conn, crawler, rank, all_data[rank])

    finally:
        try:
            crawler.page.quit()
        except Exception:
            pass
        try:
            crawler.db_conn.close()
        except Exception:
            pass

    # Post verification
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(DISTINCT bsr_rank) FROM public.amazon_tv_detail_crawled
        WHERE batch_id=%s AND bsr_rank IS NOT NULL
    """, (TARGET_DT_BATCH,))
    post_detail = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM public.amazon_tv_bsr WHERE batch_id=%s",
                (TARGET_BSR_BATCH,))
    post_bsr = cur.fetchone()[0]
    print(f"\n[POST] detail_crawled distinct bsr_rank: {pre_detail} -> {post_detail}")
    print(f"[POST] amazon_tv_bsr rows: {pre_bsr} -> {post_bsr}")

    cur.execute("""
        SELECT generate_series AS missing FROM generate_series(1, 100)
        WHERE generate_series NOT IN (
          SELECT bsr_rank FROM public.amazon_tv_detail_crawled
          WHERE batch_id=%s AND bsr_rank IS NOT NULL
        )
    """, (TARGET_DT_BATCH,))
    still_missing = [r[0] for r in cur.fetchall()]
    print(f"[POST] detail_crawled still missing (1-100): {still_missing}")
    cur.close()

    conn.close()


if __name__ == '__main__':
    main()
