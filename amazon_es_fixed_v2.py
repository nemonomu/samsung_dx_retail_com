# -*- coding: utf-8 -*-
"""
Amazon 스페인 가격 추출 시스템 - 개선판 v2.0
추천상품/관련상품 영역 필터링 대폭 강화
centerCol 내부만 타겟팅하도록 수정
스페인 특화 기능 추가
"""
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import paramiko
import time
import random
import re
from datetime import datetime
import pytz
import logging
import os
from io import StringIO
import json

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import database configuration
from config import DB_CONFIG

# 파일서버 설정
FILE_SERVER_CONFIG = {
    'host': '3.36.101.24',
    'port': 22,
    'username': 'ftpuser',
    'password': 'samsung0701!',
    'upload_path': '/home/ftpuser/uploads'
}

class AmazonESScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.country_code = 'es'
        self.korea_tz = pytz.timezone('Asia/Seoul')
        
        # 스페인 특화 선택자들 (centerCol 제한 + 강화된 필터링)
        self.selectors = {
            'price': [
                # 메인 가격 영역만 타겟팅 (우선순위 높음)
                "//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span[1]",
                "//*[@id='corePrice_feature_div']/div/div/div/div/span[1]/span[1]",
                "//*[@id='corePrice_feature_div']/div/div/span[1]/span[1]",
                
                # 메인 가격 영역 (더 구체적인 순서로) - centerCol 내부만
                "//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-offscreen']",
                "//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen']",
                "//*[@id='centerCol']//*[@id='apex_desktop']//span[@class='a-price']//span[@class='a-offscreen']",
                
                # 첫 번째 가격만 (centerCol 내부만, 더 구체적인 선택자)
                "(//*[@id='centerCol']//*[@id='corePrice_feature_div']//span[@class='a-offscreen'])[1]",
                "(//*[@id='centerCol']//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen'])[1]",
                
                # 기본 가격 요소들 (centerCol 내부만)
                "//*[@id='centerCol']//*[@id='priceblock_ourprice']",
                "//*[@id='centerCol']//*[@id='priceblock_dealprice']",
                "//*[@id='centerCol']//*[@id='listPrice']",
                
                # whole 가격 (centerCol 내부만)
                "//*[@id='centerCol']//span[@class='a-price-whole']",
                "//*[@id='centerCol']//span[@class='a-price']//span[@class='a-offscreen']",
                
                # 추가 스페인 Amazon 특화 가격 선택자들 (centerCol 제한)
                "//*[@id='centerCol']//*[@class='a-price']//span[contains(@class, 'a-offscreen')]",
                "//*[@id='centerCol']//*[contains(@class, 'a-price-whole')]",
                "//*[@id='centerCol']//*[@class='a-price a-text-price a-size-medium a-color-base']//span",
                "//*[@id='centerCol']//*[@id='price_inside_buybox']",
                
                # 백업 선택자들 (centerCol 내부만)
                "//*[@id='centerCol']//span[contains(@class, 'a-offscreen')]",
                "//*[@id='centerCol']//span[contains(text(), '€')]",
                "//*[@id='centerCol']//*[contains(@class, 'a-price')]/span[1]"
            ],
            'title': [
                "#productTitle",
                "//span[@id='productTitle']",
                "//h1/span[@id='productTitle']"
            ],
            'ships_from': [
                "//*[@id='SSOFpopoverLink_ubb']",
                "//*[@id='fulfillerInfoFeature_feature_div']/div[2]/div[1]/span",
                "//div[@id='fulfillerInfoFeature_feature_div']//span",
                "//a[@id='SSOFpopoverLink_ubb']"
            ],
            'sold_by': [
                "//*[@id='sellerProfileTriggerId']",
                "//*[@id='merchantInfoFeature_feature_div']/div[2]/div[1]/span",
                "//div[@id='merchantInfoFeature_feature_div']//a",
                "//div[@id='merchantInfoFeature_feature_div']//span",
                "//a[@id='sellerProfileTriggerId']"
            ],
            'imageurl': [
                "//div[@id='imageBlock']//img[@id='landingImage']",
                "//div[@id='main-image-container']//img",
                "//img[@class='a-dynamic-image']"
            ],
            
            # 강화된 제외 영역 (추천상품, 관련상품, 광고 등)
            'excluded_price_areas': [
                # 추천상품 관련
                'similarities_feature_div',
                'comparison_feature_div', 
                'HLCXComparisonWidget_feature_div',
                'sp_detail',
                'similarities-widget',
                'comparison-widget',
                'sp_detail_thematic',
                'aplus-module',
                'aplus',
                'a-carousel-container',
                'similarities_feature',
                'comparison_feature',
                
                # 스폰서/광고 관련
                'sponsored',
                'sp-sponsored',
                'ad-feedback',
                'adplacements',
                'sp_sponsored_',
                'a-carousel-viewport',
                'a-carousel-card',
                
                # 추가 추천 영역
                'recommended',
                'also_bought',
                'also_viewed',
                'customers_who_bought',
                'frequently_bought_together',
                'related_products',
                'product_promotions',
                'cross_promotions',
                'bundled_products',
                'combo_feature',
                
                # 기타 제외 영역
                'alternate-versions',
                'twister-plus-buying-options',
                'buying-options',
                'accessories_feature',
                'hero-quick-promo',
                'deal-cluster',
                'promotions_feature'
            ],
            
            # 고급 xpath 패턴 제외 (정규식 패턴)
            'excluded_xpath_patterns': [
                # 추천상품 xpath 패턴들
                r'.*similarities.*feature.*',
                r'.*comparison.*feature.*',
                r'.*HLCXComparisonWidget.*',
                r'.*sp_detail.*',
                r'.*aplus.*',
                r'.*carousel.*',
                r'.*sponsored.*',
                r'.*ad-.*',
                r'.*recommended.*',
                r'.*also_bought.*',
                r'.*also_viewed.*',
                r'.*frequently.*bought.*',
                r'.*customers.*bought.*',
                r'.*related.*products.*',
                r'.*accessories.*feature.*',
                r'.*hero.*promo.*',
                r'.*deal.*cluster.*',
                r'.*promotions.*feature.*',
                r'.*buying.*options.*',
                r'.*alternate.*versions.*',
                r'.*twister.*plus.*',
                r'.*bundled.*products.*',
                r'.*combo.*feature.*',
                r'.*cross.*promotions.*',
                r'.*product.*promotions.*'
            ]
        }
    
    def load_selectors_from_db(self):
        """DB에서 스페인용 선택자 로드"""
        try:
            query = """
            SELECT element_type, selector_value 
            FROM selector_configs 
            WHERE country_code = 'es' AND is_active = true
            ORDER BY priority ASC
            """
            
            df = pd.read_sql(query, self.db_engine)
            
            if not df.empty:
                logger.info(f"스페인 DB 선택자 {len(df)}개 로드")
                
                # element_type별로 그룹화하여 선택자 병합
                for element_type in df['element_type'].unique():
                    db_selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                    existing = self.selectors.get(element_type, [])
                    
                    # DB 선택자를 기존 선택자 앞에 추가 (우선순위 높임)
                    if existing:
                        self.selectors[element_type] = db_selectors + [s for s in existing if s not in db_selectors]
                    else:
                        self.selectors[element_type] = db_selectors
                
                logger.info("스페인 DB 선택자 로드 완료")
                
            except Exception as e:
                logger.error(f"DB 선택자 로드 실패: {e}")
        
        except Exception as e:
            logger.error(f"DB 선택자 로드 실패: {e}")
    
    def setup_driver(self):
        """스페인 전용 Chrome 드라이버 설정"""
        logger.info("스페인 전용 Chrome 드라이버 설정 중...")
        
        try:
            options = uc.ChromeOptions()
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-setuid-sandbox')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-software-rasterizer')
            
            # 스페인 사용자 에이전트
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            
            options.add_argument(f'--user-agent={random.choice(user_agents)}')
            options.add_argument('--accept-lang=es-ES,es;q=0.9,en;q=0.8')
            options.add_argument('--disable-extensions')
            options.add_argument('--no-first-run')
            options.add_argument('--disable-default-apps')
            options.add_argument('--disable-infobars')
            
            # 창 크기 설정
            options.add_argument('--window-size=1920,1080')
            
            self.driver = uc.Chrome(options=options)
            self.driver.set_page_load_timeout(30)
            self.driver.implicitly_wait(10)
            
            logger.info("스페인 Chrome 드라이버 설정 완료")
            
        except Exception as e:
            logger.error(f"스페인 드라이버 설정 오류: {e}")
            raise
    
    def setup_database(self):
        """스페인용 DB 연결 설정"""
        try:
            connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            self.db_engine = create_engine(connection_string, echo=False)
            logger.info("스페인용 DB 연결 성공")
        except Exception as e:
            logger.error(f"스페인 DB 연결 실패: {e}")
            raise
    
    def is_excluded_price_element(self, element):
        """가격 요소가 제외 영역인지 확인 (강화된 버전)"""
        try:
            # 1. excluded_price_areas 확인
            element_html = element.get_attribute('outerHTML').lower()
            excluded_areas = self.selectors.get('excluded_price_areas', [])
            
            for pattern in excluded_areas:
                if pattern in element_html.lower():
                    logger.debug(f"제외 영역 감지: {pattern}")
                    return True
            
            # 2. centerCol 외부의 요소는 제외
            try:
                center_col = self.driver.execute_script("""
                    return arguments[0].closest('#centerCol');
                """, element)
                
                if not center_col:
                    logger.debug("centerCol 외부 요소 제외")
                    return True
            except:
                logger.debug("centerCol 확인 중 오류")
                return True
            
            # 3. xpath 패턴 기반 제외
            try:
                element_xpath = self.driver.execute_script("""
                    function getXPath(element) {
                        var xpath = '';
                        for (; element && element.nodeType == 1; element = element.parentNode) {
                            var id = element.id;
                            if (id) {
                                xpath = '//*[@id="' + id + '"]' + xpath;
                                break;
                            } else {
                                var sameTag = [];
                                for (var i = 0; i < element.parentNode.childNodes.length; i++) {
                                    var child = element.parentNode.childNodes[i];
                                    if (child.nodeType == 1 && child.tagName == element.tagName) {
                                        sameTag.push(child);
                                    }
                                }
                                xpath = '/' + element.tagName.toLowerCase() + '[' + (sameTag.indexOf(element) + 1) + ']' + xpath;
                            }
                        }
                        return xpath;
                    }
                    return getXPath(arguments[0]);
                """, element)
                
                excluded_patterns = self.selectors.get('excluded_xpath_patterns', [])
                for pattern in excluded_patterns:
                    if re.search(pattern, element_xpath, re.IGNORECASE):
                        logger.debug(f"제외 xpath 패턴 감지: {pattern}")
                        return True
                        
            except Exception as e:
                logger.debug(f"xpath 패턴 확인 오류: {e}")
            
            # 4. 부모 요소들에서 제외 패턴 확인
            try:
                parent_elements = self.driver.execute_script("""
                    var parents = [];
                    var element = arguments[0];
                    while(element && element.parentElement) {
                        element = element.parentElement;
                        if(element.className || element.id) {
                            parents.push((element.id || '') + ' ' + (element.className || ''));
                        }
                    }
                    return parents;
                """, element)
                
                for parent_info in parent_elements:
                    parent_info = parent_info.lower()
                    for pattern in excluded_areas:
                        if pattern in parent_info:
                            logger.debug(f"부모 요소에서 제외 패턴 감지: {pattern}")
                            return True
            except:
                pass
            
            return False
            
        except Exception as e:
            logger.debug(f"제외 요소 확인 오류: {e}")
            return False
    
    def clean_ships_sold_text(self, text, element_type):
        """Ships From과 Sold By 텍스트 정리 (스페인어 지원)"""
        if not text:
            return None
            
        # 기본 정리
        text = text.strip()
        text = re.sub(r'\s+', ' ', text)
        
        # 스페인어 패턴들
        spanish_patterns = [
            'enviado por', 'vendido por', 'gestionado por', 
            'enviado desde', 'vendido desde', 'por',
            'fulfiled by', 'fulfilled by', 'sold by', 'ships from'
        ]
        
        text_lower = text.lower()
        
        # 패턴 이후의 텍스트 추출
        for pattern in spanish_patterns:
            if pattern in text_lower:
                pattern_index = text_lower.find(pattern)
                if pattern_index >= 0:
                    after_pattern = text[pattern_index + len(pattern):].strip()
                    if after_pattern:
                        return after_pattern
        
        # 특수 문자 제거
        cleaned = re.sub(r'[^\w\s\-\.&]', '', text)
        cleaned = cleaned.strip()
        
        return cleaned if cleaned else None
    
    def extract_clean_text_from_element(self, element, element_type):
        """요소에서 텍스트 추출 후 정리"""
        try:
            # 다양한 방법으로 텍스트 추출 시도
            text_methods = [
                lambda: element.get_attribute('textContent'),
                lambda: element.text,
                lambda: element.get_attribute('innerText'),
                lambda: element.get_attribute('value')
            ]
            
            for method in text_methods:
                try:
                    text = method()
                    if text and text.strip():
                        text = text.strip()
                        
                        # Ships From / Sold By 특별 처리
                        if element_type in ["Ships From", "Sold By"]:
                            return self.clean_ships_sold_text(text, element_type)
                        
                        return text
                except:
                    continue
                    
            return None
            
        except Exception as e:
            logger.debug(f"텍스트 추출 오류: {e}")
            return None
    
    def extract_element_text(self, selectors, element_name="요소"):
        """선택자 목록에서 텍스트 추출 (가격만 필터링 적용)"""
        logger.info(f"{element_name} 추출 시작 - 총 {len(selectors)}개 선택자")
        
        for idx, selector in enumerate(selectors, 1):
            try:
                logger.info(f"[{idx}/{len(selectors)}] 시도 중: {selector}")
                
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                logger.info(f"발견된 요소: {len(elements)}개")
                
                if elements:
                    for element in elements:
                        try:
                            if element.is_displayed():
                                # 가격 요소만 강화된 필터링 적용
                                if element_name == "가격" and self.is_excluded_price_element(element):
                                    continue
                                text = self.extract_clean_text_from_element(element, element_name)
                                if text:
                                    logger.info(f"최종 추출 성공: '{text}'")
                                    return text
                        except Exception as e:
                            logger.debug(f"요소 처리 중 오류: {e}")
            except Exception as e:
                logger.debug(f"선택자 처리 중 오류: {e}")
                
        logger.warning(f"{element_name} 추출 실패")
        return None
    
    def parse_spanish_price(self, price_text):
        """스페인 가격 파싱 (€ 통화, 쉼표 소수점)"""
        try:
            price_text = price_text.strip()
            logger.debug(f"스페인 가격 파싱: '{price_text}'")
            
            # 무효한 패턴 확인
            invalid_patterns = [
                r'^[a-zA-Z\s]+$',
                r'^\d+\s*[a-zA-Z]',
                r'was\s*€',
                r'list\s*price',
                r'buy\s*used'
            ]
            
            for pattern in invalid_patterns:
                if re.search(pattern, price_text, re.IGNORECASE):
                    logger.debug(f"무효한 가격 패턴 감지: {pattern}")
                    return None
            
            # 유로 기호와 공백 제거
            cleaned = re.sub(r'[€\s]', '', price_text)
            logger.debug(f"통화 제거 후: '{cleaned}'")
            
            # 빈 문자열 체크
            if not cleaned:
                return None
            
            # 끝에 점만 있는 경우 처리 (예: "294." -> "294")
            if cleaned.endswith('.') and len(cleaned) > 1:
                cleaned_no_dot = cleaned[:-1]
                if cleaned_no_dot.isdigit():
                    logger.debug(f"끝 점 제거: '{cleaned}' -> '{cleaned_no_dot}'")
                    cleaned = cleaned_no_dot
            
            # 스페인 형식: 61,84 또는 1.234,56
            if ',' in cleaned:
                # 쉼표가 있으면 마지막 쉼표를 소수점으로 처리
                parts = cleaned.rsplit(',', 1)
                if len(parts) == 2 and len(parts[1]) <= 2 and parts[1].isdigit():
                    # 정수 부분에서 점 제거 (천단위 구분자)
                    integer_part = parts[0].replace('.', '')
                    decimal_part = parts[1]
                    final_price = f"{integer_part}.{decimal_part}"
                    logger.debug(f"스페인 쉼표→점 변환: '{final_price}'")
                    
                    # 최종 검증
                    if re.match(r'^\d+\.\d{1,2}$', final_price):
                        return final_price
                else:
                    # 쉼표가 있지만 소수점이 아닌 경우
                    cleaned = cleaned.replace(',', '')
            
            # 정수 가격 또는 점으로 구분된 가격
            if re.match(r'^\d+(\.\d{1,2})?$', cleaned):
                try:
                    price_value = float(cleaned)
                    logger.info(f"스페인 최종 가격 검증: '{cleaned}' -> {price_value}")
                    if 1 <= price_value <= 50000:
                        logger.info(f"스페인 가격 파싱 성공: {cleaned}")
                        return cleaned
                    else:
                        logger.debug(f"가격 범위 벗어남: {price_value}")
                except Exception as e:
                    logger.debug(f"float 변환 실패: {cleaned} - {e}")
                    pass
                
        except Exception as e:
            logger.debug(f"스페인 가격 파싱 오류: {price_text} - {e}")
            
        return None
    
    def extract_price(self):
        """스페인 가격 추출 (개선된 버전) - 메인 상품 영역만 타겟팅"""
        logger.info("스페인 가격 추출 시작 (메인 상품 영역만)")
        
        # 1단계: 메인 신품 가격 추출 (centerCol 내부만)
        price_selectors = self.selectors.get('price', [])
        
        for idx, selector in enumerate(price_selectors, 1):
            try:
                logger.info(f"[신품 {idx}/{len(price_selectors)}] 시도: {selector}")
                
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    try:
                        if element.is_displayed():
                            if self.is_excluded_price_element(element):
                                continue
                            
                            text = self.extract_clean_text_from_element(element, "가격")
                            if text:
                                logger.info(f"스페인 1단계 추출된 텍스트: '{text}'")
                                
                                
                                price = self.parse_spanish_price(text)
                                if price:
                                    logger.info(f"스페인 가격 추출 성공: {price}")
                                    return price
                                else:
                                    logger.debug(f"파싱 실패한 텍스트: '{text}'")
                    except Exception as e:
                        logger.debug(f"요소 처리 오류: {e}")
                        continue
            except Exception as e:
                logger.debug(f"선택자 오류: {e}")
                continue
        
        
        logger.error("스페인 모든 방법으로 가격 추출 실패 (메인 상품 영역만 검색)")
        return None
    
    def check_stock_availability(self):
        """스페인 재고 상태 확인"""
        try:
            # 스페인 availability 요소 확인
            try:
                availability_elem = self.driver.find_element(By.ID, "availability")
                if availability_elem:
                    availability_text = availability_elem.text.lower()
                    logger.info(f"스페인 재고 상태 텍스트: {availability_text}")
                    
                    # 스페인어 재고 없음 패턴
                    out_of_stock_patterns = [
                        'no disponible', 'agotado', 'sin stock', 'fuera de stock',
                        'temporarily out of stock', 'out of stock', 'unavailable'
                    ]
                    
                    for pattern in out_of_stock_patterns:
                        if pattern in availability_text:
                            logger.info(f"스페인 재고 없음 감지: {pattern}")
                            return False
                    
                    return True
            except NoSuchElementException:
                pass
            
            # 백업 재고 확인 방법들
            stock_selectors = [
                "#availability span",
                ".a-size-medium.a-color-success",
                "#availability .a-size-medium",
                "#buybox .a-color-success"
            ]
            
            for selector in stock_selectors:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element and element.is_displayed():
                        text = element.text.lower()
                        logger.info(f"스페인 재고 확인 텍스트: {text}")
                        
                        if any(pattern in text for pattern in ['no disponible', 'agotado', 'sin stock', 'out of stock']):
                            return False
                        elif any(pattern in text for pattern in ['disponible', 'en stock', 'in stock', 'available']):
                            return True
                except:
                    continue
            
            logger.info("스페인 재고 상태 확인 불가 - 기본값 True")
            return True
            
        except Exception as e:
            logger.error(f"스페인 재고 확인 오류: {e}")
            return True

    def extract_product_info(self, url, row_data, retry_count=0, max_retries=2):
        """스페인 상품 정보 추출 (강화된 버전)"""
        logger.info(f"스페인 상품 정보 추출 시작: {url}")
        
        try:
            # 페이지 로드
            self.driver.get(url)
            
            # 페이지 로드 대기
            wait_time = random.uniform(3, 6)
            logger.info(f"스페인 {wait_time:.1f}초 대기 중...")
            time.sleep(wait_time)
            
            # 한국 시간 설정
            now_time = datetime.now(self.korea_tz)
            
            # 기본 결과 구조 
            result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': 'es',
                'ships_from': None,
                'channel': row_data.get('channel', 'Online'),
                'retailersku': row_data.get('retailersku', ''),
                'brand': row_data.get('brand', ''),
                'brand_eng': row_data.get('brand_eng', row_data.get('brand', '')),
                'form_factor': row_data.get('form_factor', ''),
                'segment_lv1': row_data.get('seg_lv1', ''),
                'segment_lv2': row_data.get('seg_lv2', ''),
                'segment_lv3': row_data.get('seg_lv3', ''),
                'capacity': row_data.get('capacity', ''),
                'item': row_data.get('item', ''),
                'retailprice': None,
                'sold_by': None,
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o'),
            }
            
            # 재고 확인
            has_stock = self.check_stock_availability()
            logger.info(f"스페인 재고 상태: {has_stock}")
            
            # Ships From 추출
            result['ships_from'] = self.extract_element_text(
                self.selectors.get('ships_from', []), 
                "Ships From"
            )
            
            # Sold By 추출 (기본 추출만, 추가 필터링 없음)
            result['sold_by'] = self.extract_element_text(
                self.selectors.get('sold_by', []), 
                "Sold By"
            )
            
            # Ships From과 Sold By가 모두 없으면 가격도 빈 값으로 처리
            if not result['ships_from'] and not result['sold_by']:
                logger.warning("Ships From과 Sold By가 모두 없음 - 가격을 빈 값으로 설정")
                result['retailprice'] = None
            else:
                # 가격 추출 (개선된 메서드 사용 - 메인 영역만)
                result['retailprice'] = self.extract_price()
                
                # 가격 범위 검증
                if result['retailprice']:
                    try:
                        price_value = float(re.sub(r'[^\d.]', '', str(result['retailprice'])))
                        if price_value < 1 or price_value > 50000:
                            logger.warning(f"스페인 비정상적인 가격: {result['retailprice']}")
                            result['retailprice'] = None
                    except:
                        result['retailprice'] = None
                
                # 재고 없을 때 가격 처리
                if not has_stock and result['retailprice'] is None:
                    result['retailprice'] = None
            
            # 제목 추출
            result['title'] = self.extract_element_text(
                self.selectors.get('title', []), 
                "제목"
            )
            
            # 이미지 URL 추출
            result['imageurl'] = self.extract_element_text(
                self.selectors.get('imageurl', []), 
                "이미지"
            )
            
            # 추출 결과 로그
            logger.info("스페인 추출 결과:")
            logger.info(f"제목: {result['title']}")
            logger.info(f"가격: {result['retailprice']}")
            logger.info(f"이미지: {'있음' if result['imageurl'] else '없음'}")
            logger.info(f"판매자: {result['sold_by']}")
            logger.info(f"배송지: {result['ships_from']}")
            
            return result
            
        except Exception as e:
            logger.error(f"스페인 페이지 처리 오류: {e}")
            
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10
                logger.info(f"{wait_time}초 후 재시도... ({retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
            
            now_time = datetime.now(self.korea_tz)
            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': 'es',
                'ships_from': None,
                'channel': row_data.get('channel', 'Online'),
                'retailersku': row_data.get('retailersku', ''),
                'brand': row_data.get('brand', ''),
                'brand_eng': row_data.get('brand_eng', row_data.get('brand', '')),
                'form_factor': row_data.get('form_factor', ''),
                'segment_lv1': row_data.get('seg_lv1', ''),
                'segment_lv2': row_data.get('seg_lv2', ''),
                'segment_lv3': row_data.get('seg_lv3', ''),
                'capacity': row_data.get('capacity', ''),
                'item': row_data.get('item', ''),
                'retailprice': None,
                'sold_by': None,
                'imageurl': None,
                'producturl': url,
                'crawl_datetime': now_time.strftime('%Y-%m-%d %H:%M:%S'),
                'crawl_strdatetime': now_time.strftime('%Y%m%d%H%M%S') + f"{now_time.microsecond:06d}"[:4],
                'title': None,
                'vat': row_data.get('vat', 'o'),
            }

# 실행 부분은 기존과 동일하게 유지
if __name__ == "__main__":
    logger.info("스페인 Amazon 크롤러 시작")
    
    scraper = AmazonESScraper()
    
    try:
        scraper.setup_database()
        scraper.load_selectors_from_db()
        scraper.setup_driver()
        
        # 테스트용 URL (실제 사용 시 수정 필요)
        test_url = "https://www.amazon.es/dp/B08N5WRWNW"
        test_data = {
            'retailerid': 'test',
            'brand': 'Samsung',
            'capacity': '1TB'
        }
        
        result = scraper.extract_product_info(test_url, test_data)
        logger.info(f"스페인 테스트 결과: {result}")
        
    except Exception as e:
        logger.error(f"스페인 크롤러 실행 오류: {e}")
    
    finally:
        if scraper.driver:
            scraper.driver.quit()
        logger.info("스페인 크롤러 종료")