"""
Data Validation Module for BestBuy TV Crawlers
기존 데이터와 다른 형태의 값 감지 및 로깅

검증 항목:
1. NULL 값 검증 (필수 컬럼)
2. 형식 검증 (price는 $, item은 패턴 확인)
3. 이상치 검증 (price $0 또는 비정상 범위)
"""

import os
import re
from datetime import datetime


class DataValidator:
    """데이터 검증 및 문제 로깅 클래스"""

    def __init__(self, session_start_time):
        """
        Args:
            session_start_time: YYYYMMDDHHMM 형식 (예: 202511151200)
        """
        self.session_start_time = session_start_time
        self.problems_dir = r"C:\samsung_dx_retail_com\problems"
        self.log_file = None
        self.issue_count = 0
        self.issues_by_type = {}  # 타입별 이슈 카운트

    def _ensure_log_file(self):
        """로그 디렉토리 및 파일 생성"""
        if self.log_file is None:
            os.makedirs(self.problems_dir, exist_ok=True)
            self.log_file = os.path.join(self.problems_dir, f"{self.session_start_time}.txt")

            # 파일 헤더 작성
            with open(self.log_file, 'w', encoding='utf-8') as f:
                f.write("="*80 + "\n")
                f.write(f"DATA VALIDATION LOG\n")
                f.write(f"Session Start: {self.session_start_time}\n")
                f.write("="*80 + "\n\n")

    def log_issue(self, crawler_name, product_url, field_name, value, issue_type, expected=None):
        """
        문제를 파일 및 콘솔에 로깅

        Args:
            crawler_name: 크롤러 이름 (예: bby_tv_main1)
            product_url: 제품 URL
            field_name: 문제가 있는 컬럼명
            value: 문제가 있는 값
            issue_type: NULL_VALUE, FORMAT_ISSUE, OUTLIER
            expected: 기대되는 형식 (선택)
        """
        self._ensure_log_file()

        # 파일에 로깅
        with open(self.log_file, 'a', encoding='utf-8') as f:
            timestamp = datetime.now().strftime('%H:%M:%S')
            f.write(f"[{timestamp}] {issue_type}\n")
            f.write(f"  Crawler: {crawler_name}\n")
            f.write(f"  Field: {field_name}\n")
            f.write(f"  Value: {repr(value)}\n")
            if expected:
                f.write(f"  Expected: {expected}\n")
            f.write(f"  URL: {product_url}\n")
            f.write("\n")

        # 카운트 증가
        self.issue_count += 1
        self.issues_by_type[issue_type] = self.issues_by_type.get(issue_type, 0) + 1

        # 콘솔에도 WARNING 출력
        print(f"  [WARNING] {issue_type}: {field_name} = {repr(value)}")

    def validate_item(self, item, product_url, crawler_name):
        """
        item 검증
        - NULL 체크
        - 형식 체크 (모델명 포함 여부)
        """
        if item is None or item == '' or str(item).strip() == '':
            self.log_issue(crawler_name, product_url, 'item', item, 'NULL_VALUE',
                          'Should have item value (e.g., "55 inch TV")')
            return False

        # "none"이라는 문자열인 경우
        if str(item).lower() == 'none':
            self.log_issue(crawler_name, product_url, 'item', item, 'NULL_VALUE',
                          'Item should not be "none"')
            return False

        # item에 "inch"만 있고 모델명/TV가 없는 경우 (예: "24 inch")
        item_str = str(item).lower()
        if 'inch' in item_str:
            # "inch" 제거 후 영문자가 거의 없으면 의심
            without_inch = item_str.replace('inch', '').replace('inches', '')
            alpha_count = sum(c.isalpha() for c in without_inch)
            if alpha_count < 2:  # 영문자가 2개 미만
                self.log_issue(crawler_name, product_url, 'item', item, 'FORMAT_ISSUE',
                              'Should include model name or "TV" (e.g., "55 inch Smart TV")')
                return False

        return True

    def validate_price(self, price, field_name, product_url, crawler_name):
        """
        가격 검증
        - NULL 체크
        - $ 기호 확인
        - 숫자 범위 확인
        """
        if price is None or price == '' or str(price).strip() == '':
            self.log_issue(crawler_name, product_url, field_name, price, 'NULL_VALUE',
                          'Should have price value (e.g., "$599.99")')
            return False

        price_str = str(price).strip()

        # "none" 문자열 체크
        if price_str.lower() == 'none':
            self.log_issue(crawler_name, product_url, field_name, price, 'NULL_VALUE',
                          'Price should not be "none"')
            return False

        # $ 기호 확인
        if not price_str.startswith('$'):
            self.log_issue(crawler_name, product_url, field_name, price, 'FORMAT_ISSUE',
                          'Should start with $ (e.g., "$599.99")')
            return False

        # 숫자 추출 및 범위 확인
        try:
            price_num = float(price_str.replace('$', '').replace(',', ''))

            # 이상치 검증
            if price_num <= 0:
                self.log_issue(crawler_name, product_url, field_name, price, 'OUTLIER',
                              'Price should be greater than $0')
                return False

            if price_num > 50000:  # TV 가격이 $50,000 넘으면 이상
                self.log_issue(crawler_name, product_url, field_name, price, 'OUTLIER',
                              'Price seems unusually high (> $50,000)')
                return False

        except ValueError:
            self.log_issue(crawler_name, product_url, field_name, price, 'FORMAT_ISSUE',
                          'Cannot parse price value')
            return False

        return True

    def validate_screen_size(self, screen_size, product_url, crawler_name):
        """
        screen_size 검증
        - NULL 체크
        - "inch" 포함 확인
        - 크기 범위 확인 (20-100 inch)
        """
        if screen_size is None or screen_size == '' or str(screen_size).strip() == '':
            self.log_issue(crawler_name, product_url, 'screen_size', screen_size, 'NULL_VALUE',
                          'Should have screen size (e.g., "55 inch")')
            return False

        screen_size_str = str(screen_size).lower()

        # "inch" 포함 확인
        if 'inch' not in screen_size_str:
            self.log_issue(crawler_name, product_url, 'screen_size', screen_size, 'FORMAT_ISSUE',
                          'Should include "inch" (e.g., "55 inch")')
            return False

        # 숫자 추출
        match = re.search(r'(\d+)', screen_size_str)
        if match:
            size = int(match.group(1))
            if size < 20 or size > 100:  # TV 크기 범위
                self.log_issue(crawler_name, product_url, 'screen_size', screen_size, 'OUTLIER',
                              'Screen size seems unusual (expected 20-100 inch)')
                return False
        else:
            self.log_issue(crawler_name, product_url, 'screen_size', screen_size, 'FORMAT_ISSUE',
                          'Cannot extract size number')
            return False

        return True

    def validate_count(self, count_value, field_name, product_url, crawler_name):
        """
        count 계열 검증 (count_of_reviews 등)
        - 숫자 형식 확인
        - 음수 체크
        """
        # NULL은 허용 (리뷰가 없을 수 있음)
        if count_value is None or count_value == '' or str(count_value).strip() == '':
            return True

        # "none" 문자열은 경고
        if str(count_value).lower() == 'none':
            return True  # 허용하지만 로그는 남기지 않음

        # 숫자인지 확인
        try:
            count_num = int(str(count_value).replace(',', ''))
            if count_num < 0:
                self.log_issue(crawler_name, product_url, field_name, count_value, 'OUTLIER',
                              'Count should not be negative')
                return False
        except ValueError:
            self.log_issue(crawler_name, product_url, field_name, count_value, 'FORMAT_ISSUE',
                          'Should be numeric')
            return False

        return True

    def validate_star_rating(self, rating, product_url, crawler_name):
        """
        star_rating 검증
        - 0-5 범위 확인
        """
        # NULL 허용 (리뷰가 없을 수 있음)
        if rating is None or rating == '' or str(rating).strip() == '':
            return True

        # "none" 허용
        if str(rating).lower() == 'none':
            return True

        try:
            rating_num = float(rating)
            if rating_num < 0 or rating_num > 5:
                self.log_issue(crawler_name, product_url, 'star_rating', rating, 'OUTLIER',
                              'Rating should be between 0 and 5')
                return False
        except ValueError:
            self.log_issue(crawler_name, product_url, 'star_rating', rating, 'FORMAT_ISSUE',
                          'Should be numeric (0-5)')
            return False

        return True

    def get_issue_count(self):
        """총 이슈 개수 반환"""
        return self.issue_count

    def get_summary(self):
        """이슈 요약 정보 반환"""
        return {
            'total': self.issue_count,
            'by_type': self.issues_by_type.copy()
        }

    def write_summary(self):
        """로그 파일에 요약 정보 작성"""
        if self.log_file and self.issue_count > 0:
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write("\n" + "="*80 + "\n")
                f.write("SUMMARY\n")
                f.write("="*80 + "\n")
                f.write(f"Total Issues: {self.issue_count}\n")
                for issue_type, count in sorted(self.issues_by_type.items()):
                    f.write(f"  {issue_type}: {count}\n")
                f.write("="*80 + "\n")
