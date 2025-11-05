"""
bby_tv_bsr_page_url 테이블 생성 및 URL 데이터 삽입
Best Buy Best-Selling TV 페이지 URL (1~8페이지)
"""
import psycopg2
from datetime import datetime
from config import DB_CONFIG

def create_bby_tv_bsr_page_url_table():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()

        print("="*80)
        print("bby_tv_bsr_page_url 테이블 생성 및 데이터 삽입")
        print("="*80)

        # 테이블 생성
        print("\n[1] 테이블 생성 중...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bby_tv_bsr_page_url (
                id SERIAL PRIMARY KEY,
                page_number INTEGER NOT NULL,
                url TEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("  [OK] bby_tv_bsr_page_url 테이블 생성 완료")

        # 기존 데이터 확인
        cursor.execute("SELECT COUNT(*) FROM bby_tv_bsr_page_url")
        count = cursor.fetchone()[0]

        if count > 0:
            print(f"\n[INFO] 기존 데이터 {count}개 존재")
            cursor.execute("DELETE FROM bby_tv_bsr_page_url")
            print("  [OK] 기존 데이터 삭제 완료")

        # URL 데이터 삽입
        print("\n[2] URL 데이터 삽입 중...")

        urls = [
            (1, "https://www.bestbuy.com/site/searchpage.jsp?id=pcat17071&sp=Best-Selling&st=tv"),
            (2, "https://www.bestbuy.com/site/searchpage.jsp?cp=2&id=pcat17071&sp=Best-Selling&st=tv"),
            (3, "https://www.bestbuy.com/site/searchpage.jsp?cp=3&id=pcat17071&sp=Best-Selling&st=tv"),
            (4, "https://www.bestbuy.com/site/searchpage.jsp?cp=4&id=pcat17071&sp=Best-Selling&st=tv"),
            (5, "https://www.bestbuy.com/site/searchpage.jsp?cp=5&id=pcat17071&sp=Best-Selling&st=tv"),
            (6, "https://www.bestbuy.com/site/searchpage.jsp?cp=6&id=pcat17071&sp=Best-Selling&st=tv"),
            (7, "https://www.bestbuy.com/site/searchpage.jsp?cp=7&id=pcat17071&sp=Best-Selling&st=tv"),
            (8, "https://www.bestbuy.com/site/searchpage.jsp?cp=8&id=pcat17071&sp=Best-Selling&st=tv")
        ]

        insert_query = """
            INSERT INTO bby_tv_bsr_page_url (page_number, url, is_active)
            VALUES (%s, %s, %s)
        """

        for page_number, url in urls:
            cursor.execute(insert_query, (page_number, url, True))
            print(f"  [OK] Page {page_number}: {url[:80]}...")

        # 커밋
        conn.commit()
        print("\n" + "="*80)
        print("[SUCCESS] 테이블 생성 및 데이터 삽입 완료!")
        print("="*80)

        # 결과 확인
        print("\n[결과 확인]")
        cursor.execute("""
            SELECT page_number, url, is_active, created_at
            FROM bby_tv_bsr_page_url
            ORDER BY page_number
        """)

        rows = cursor.fetchall()
        print(f"\n총 {len(rows)}개 URL 등록:")
        for page_num, url, is_active, created_at in rows:
            print(f"  Page {page_num}: {url[:80]}... (Active: {is_active})")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n[ERROR] 작업 실패: {e}")
        if conn:
            conn.rollback()
            conn.close()
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    create_bby_tv_bsr_page_url_table()
