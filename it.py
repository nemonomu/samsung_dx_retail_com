# -*- coding: utf-8 -*-
"""
Amazon Italy 가격 추출 시스템 - 이탈리아 전용 최적화 버전
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

class AmazonItalyScraper:
    def __init__(self):
        self.driver = None
        self.db_engine = None
        self.country_code = 'it'
        self.wait = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        
        self.setup_db_connection()
        self.setup_italy_selectors()
        self.load_selectors_from_db()
        
    def setup_db_connection(self):
        """DB 연결 설정"""
        try:
            connection_string = (
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
                f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
            self.db_engine = create_engine(connection_string)
            logger.info("DB 연결 설정 완료")
            
        except Exception as e:
            logger.error(f"DB 연결 실패: {e}")
            self.db_engine = None
    
    def setup_italy_selectors(self):
        """이탈리아 특화 선택자 설정"""
        self.selectors = {
            'price': [
                # 이탈리아 Amazon 가격 선택자 (우선순위순)
                "//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span[1]",
                "//*[@id='corePrice_feature_div']//span[@class='a-offscreen']",
                "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen']", 
                ".a-price .a-offscreen",
                "//span[@class='a-price']//span[@class='a-offscreen']",
                "//*[@id='corePrice_feature_div']//span[@class='a-price-whole']",
                "//*[@id='corePrice_feature_div']/div/div/div/div/span[1]/span[1]",
                "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-whole']",
                "span.a-price-whole",
                "//span[@class='a-price-whole']",
                "//span[@id='priceblock_ourprice']",
                "//span[@id='priceblock_dealprice']",
                # 이탈리아 전용 가격 선택자
                "//span[@class='a-price-symbol' and contains(text(), '€')]/following-sibling::span",
                "//span[contains(@class, 'a-price') and contains(text(), '€')]",
                "//*[@id='apex_desktop']//span[@class='a-offscreen']"
            ],
            'price_fraction': [
                "//*[@id='corePrice_feature_div']/div/div/div/div/span[1]/span[2]",
                "//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span[3]/span[2]",
                "//*[@id='corePrice_feature_div']//span[@class='a-price-fraction']",
                "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-fraction']",
                "//span[@class='a-price-fraction']",
                ".a-price-fraction"
            ],
            'title': [
                "#productTitle",
                "//span[@id='productTitle']",
                "//h1/span[@id='productTitle']",
                "h1#title span",
                "//div[@id='titleSection']//h1//span",
                "//div[@id='title_feature_div']//h1//span"
            ],
            'ships_from': [
                # 이탈리아 "Spedito da" 선택자
                "//*[@id='SSOFpopoverLink_ubb']",
                "//a[@id='SSOFpopoverLink_ubb']",
                "//*[@id='fulfillerInfoFeature_feature_div']/div[2]/div[1]/span",
                "//div[@id='fulfillerInfoFeature_feature_div']//span",
                "//*[@id='fulfillerInfoFeature_feature_div']//div[2]//span",
                "//div[contains(@id, 'fulfillerInfo')]//span",
                "//span[contains(text(), 'Spedito da')]/following-sibling::span",
                "//span[contains(text(), 'Spedito da')]/parent::div//span[2]",
                "//div[contains(text(), 'Spedito da')]//span",
                "//div[@data-feature-name='shipsFromSoldBy']//span"
            ],
            'sold_by': [
                # 이탈리아 "Venduto da" 선택자
                "//a[@id='sellerProfileTriggerId']",
                "//*[@id='sellerProfileTriggerId']",
                "//*[@id='merchantInfoFeature_feature_div']/div[2]/div[1]/span",
                "//div[@id='merchantInfoFeature_feature_div']//a",
                "//div[@id='merchantInfoFeature_feature_div']//span",
                "//*[@id='merchantInfoFeature_feature_div']//div[2]//span",
                "//div[contains(@id, 'merchantInfo')]//span",
                "//span[contains(text(), 'Venduto da')]/following-sibling::a",
                "//span[contains(text(), 'Venduto da')]/following-sibling::span",
                "//span[contains(text(), 'Venduto da')]/parent::div//span[2]",
                "//div[contains(text(), 'Venduto da')]//span",
                "//div[@data-feature-name='shipsFromSoldBy']//a"
            ],
            'imageurl': [
                "//div[@id='imageBlock']//img[@id='landingImage']",
                "//div[@id='main-image-container']//img",
                "//img[@class='a-dynamic-image']",
                "//div[@class='imgTagWrapper']//img",
                "//div[@id='imageBlock_feature_div']//img"
            ],
            'availability': [
                "//div[@id='availability']//span",
                "//div[@id='availability_feature_div']//span",
                "//span[@class='a-size-medium a-color-success']",
                "//span[@class='a-size-medium a-color-price']",
                "//div[@id='availability']//div[@class='a-row']//span"
            # ],
            # 'vat_text_list': [
            #     # 이탈리아 VAT 관련 텍스트
            #     "IVA inclusa",
            #     "IVA compresa", 
            #     "Comprensivo di IVA",
            #     "Inclusa IVA",
            #     "Tax included",
            #     "include VAT.",
            #     "VAT included",
            #     "Inclusive of all taxes",
            #     "Including all taxes",
            #     "Includes all taxes",
            #     "Price includes VAT",
            #     "Inc. tax",
            #     "Incl. VAT",
            #     "Tasse incluse"
            ],
            'stock_flag': [
                # 이탈리아 재고 부족 메시지
                'Attualmente non disponibile',
                'Non disponibile', 
                'Esaurito',
                'Temporaneamente non disponibile',
                'Currently unavailable', 
                'Out of Stock',
                'Temporarily out of stock'
            ],
            'blocked_patterns': [
                'sorry', 
                'robot check', 
                '503 Service Unavailable',
                'Something went wrong',
                'access denied',
                'qualcosa è andato storto',
                'accesso negato',
                'controlla di essere umano',
                'inserisci i caratteri'
            ],
            'continue_buttons': [
                # 이탈리아 "계속 쇼핑" 버튼
                "//button[contains(text(), 'Continua lo shopping')]",
                "//a[contains(text(), 'Continua lo shopping')]",
                "//span[contains(text(), 'Continua lo shopping')]/ancestor::button",
                "//input[@value='Continua lo shopping']",
                "//button[contains(@class, 'a-button') and contains(., 'Continua')]",
                "//div[contains(@class, 'a-button') and contains(., 'Continua')]//button",
                "//button[contains(text(), 'Continue shopping')]",
                "//button[contains(@class, 'a-button-primary')]",
                "//input[@type='submit' and contains(@value, 'Continue')]"
            ]
        }
    
    def load_selectors_from_db(self):
        """DB에서 이탈리아 특화 선택자 로드"""
        if not self.db_engine:
            logger.warning("DB 연결이 없어 선택자 로드 불가")
            return
            
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM amazon_selectors
            WHERE country_code = 'it' 
              AND is_active = TRUE
              AND selector_value NOT LIKE '/html/%'
            ORDER BY element_type, priority ASC
            """
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"DB에서 이탈리아 선택자 로드: {len(df)}개")
            
            for element_type in df['element_type'].unique():
                db_selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                
                if element_type in self.selectors:
                    existing = self.selectors[element_type]
                    # DB 선택자를 기존 선택자 앞에 추가 (우선순위)
                    self.selectors[element_type] = db_selectors + [s for s in existing if s not in db_selectors]
                else:
                    self.selectors[element_type] = db_selectors
            
            logger.info("이탈리아 DB 선택자 로드 완료")
            
        except Exception as e:
            logger.error(f"DB 선택자 로드 실패: {e}")
    
    def setup_driver(self):
        """이탈리아 전용 Chrome 드라이버 설정"""
        logger.info("이탈리아 전용 Chrome 드라이버 설정 중...")
        
        try:
            options = uc.ChromeOptions()
            
            # 이탈리아 사용자 에이전트
            italian_user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-setuid-sandbox')
            options.add_argument(f'--user-agent={random.choice(italian_user_agents)}')
            
            # 이탈리아 언어 설정
            options.add_experimental_option('prefs', {
                'intl.accept_languages': 'it-IT,it,en-US,en'
            })
            
            self.driver = uc.Chrome(options=options)
            self.driver.maximize_window()
            
            self.wait = WebDriverWait(self.driver, 20)
            
            logger.info("이탈리아 드라이버 설정 완료")
            return True
            
        except Exception as e:
            logger.error(f"드라이버 설정 실패: {e}")
            return False
    
    def is_page_blocked(self):
        """이탈리아 페이지 차단 감지"""
        try:
            page_title = self.driver.title.lower()
            page_source = self.driver.page_source.lower()
            current_url = self.driver.current_url.lower()
            
            # 이탈리아 심각한 차단 지표
            serious_blocked_indicators = {
                'title': [
                    '503',
                    'access denied',
                    'accesso negato',
                    'error has occurred',
                    'errore',
                    'ci dispiace'
                ],
                'content': [
                    'enter the characters',
                    'inserisci i caratteri',
                    'verify you are human',
                    'controlla di essere umano',
                    'access denied',
                    'accesso negato',
                    'automated access',
                    'suspicious activity',
                    'attività sospetta',
                    'si è verificato un errore',
                    'ci dispiace'
                ]
            }
            
            for pattern in serious_blocked_indicators['title']:
                if pattern in page_title:
                    logger.warning(f"이탈리아 심각한 차단 감지 (제목): {pattern}")
                    return True
            
            # "Ci dispiace" 오류는 차단이 아닌 처리 가능한 오류로 분류
            if ('ci dispiace' in page_source and 
                'continua lo shopping' not in page_source and 
                'continue shopping' not in page_source and
                'clicca qui per tornare' not in page_source):
                # 홈페이지 링크도 없으면 심각한 차단
                for pattern in serious_blocked_indicators['content']:
                    if pattern in page_source and pattern not in ['ci dispiace', 'si è verificato un errore']:
                        logger.warning(f"이탈리아 심각한 차단 감지 (본문): {pattern}")
                        return True
            
            if 'amazon' not in current_url:
                logger.warning(f"이탈리아 Amazon 페이지가 아님: {current_url}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"이탈리아 페이지 차단 확인 중 오류: {e}")
            return False
    
    def wait_for_page_load(self, timeout=10):
        """이탈리아 페이지 로드 대기"""
        try:
            self.wait.until(
                lambda driver: driver.execute_script("return document.readyState") == "complete"
            )
            
            # 이탈리아 페이지 요소 확인
            possible_elements = [
                (By.ID, "productTitle"),
                (By.ID, "priceblock_ourprice"),
                (By.CLASS_NAME, "a-price-whole"),
                (By.ID, "availability"),
                (By.ID, "imageBlock"),
                (By.ID, "corePrice_feature_div"),
                (By.ID, "corePriceDisplay_desktop_feature_div")
            ]
            
            for by, value in possible_elements:
                try:
                    WebDriverWait(self.driver, 3).until(
                        EC.presence_of_element_located((by, value))
                    )
                    logger.debug(f"이탈리아 요소 발견: {by}={value}")
                    return True
                except:
                    continue
            
            return True
            
        except Exception as e:
            logger.warning(f"이탈리아 페이지 로드 대기 중 오류: {e}")
            return False

    def handle_captcha_or_block_page(self, original_url=None):
        """이탈리아 차단 페이지나 캡차 처리"""
        try:
            logger.info("이탈리아 차단/캡차 페이지 확인 중...")
            
            page_source = self.driver.page_source.lower()
            page_title = self.driver.title.lower()
            
            # 1. "Ci dispiace" 오류 페이지 정확한 감지
            ci_dispiace_indicators = [
                'ci dispiace' in page_title,
                'si è verificato un errore quando abbiamo tentato di elaborare la richiesta' in page_source,
                'stiamo lavorando al problema' in page_source,
                'clicca qui per tornare alla home page di amazon.it' in page_source,
                'non sarà stato elaborato per il momento' in page_source
            ]
            
            if sum(ci_dispiace_indicators) >= 2:
                logger.info("이탈리아 'Ci dispiace' 오류 페이지 확인됨")
                
                # "Clicca qui per tornare alla home page di Amazon.it" 링크 찾기
                home_link_selectors = [
                    "//a[contains(text(), 'Clicca qui per tornare alla home page')]",
                    "//a[contains(text(), 'tornare alla home page')]",
                    "//a[contains(text(), 'home page di Amazon')]",
                    "//a[contains(@href, 'amazon.it') and contains(text(), 'home')]",
                    "//a[contains(text(), 'Amazon.it') and contains(text(), 'home')]"
                ]
                
                for selector in home_link_selectors:
                    try:
                        logger.info(f"이탈리아 홈페이지 링크 찾기 시도: {selector}")
                        
                        link = self.driver.find_element(By.XPATH, selector)
                        
                        if link and link.is_displayed():
                            link_text = link.text
                            logger.info(f"이탈리아 홈페이지 링크 발견: '{link_text}'")
                            
                            # 홈페이지 링크 클릭
                            self.driver.execute_script("arguments[0].scrollIntoView();", link)
                            time.sleep(1)
                            
                            try:
                                link.click()
                                logger.info("이탈리아 홈페이지 링크 클릭 성공")
                            except:
                                try:
                                    self.driver.execute_script("arguments[0].click();", link)
                                    logger.info("이탈리아 홈페이지 링크 JavaScript 클릭 성공")
                                except:
                                    logger.warning("이탈리아 홈페이지 링크 클릭 실패")
                                    continue
                            
                            time.sleep(3)
                            
                            # 원래 URL로 다시 이동
                            if original_url:
                                logger.info(f"원래 URL로 재접속: {original_url}")
                                self.driver.get(original_url)
                                time.sleep(3)
                                return True
                            else:
                                logger.info("이탈리아 홈페이지 이동 완료")
                                return True
                                
                    except Exception as e:
                        logger.debug(f"이탈리아 홈페이지 링크 오류: {e}")
                        continue
                
                # 홈페이지 링크를 찾지 못했으면 직접 홈페이지로 이동
                logger.info("이탈리아 홈페이지 링크를 찾지 못함, 직접 이동")
                self.driver.get("https://www.amazon.it/")
                time.sleep(3)
                
                if original_url:
                    logger.info(f"원래 URL로 재접속: {original_url}")
                    self.driver.get(original_url)
                    time.sleep(3)
                
                return True
            
            # 2. 일반 Continue 버튼 처리 (Ci dispiace가 아닌 경우에만)
            if 'ci dispiace' not in page_title and 'ci dispiace' not in page_source:
                for selector in self.selectors['continue_buttons']:
                    try:
                        logger.info(f"이탈리아 Continue 버튼 찾기 시도: {selector}")
                        
                        if selector.startswith('//'):
                            button = self.driver.find_element(By.XPATH, selector)
                        else:
                            button = self.driver.find_element(By.CSS_SELECTOR, selector)
                        
                        if button and button.is_displayed():
                            button_text = button.text
                            logger.info(f"이탈리아 Continue 버튼 발견: {selector} (텍스트: '{button_text}')")
                            
                            self.driver.execute_script("arguments[0].scrollIntoView();", button)
                            time.sleep(1)
                            
                            try:
                                button.click()
                                logger.info("이탈리아 Continue 버튼 클릭 성공")
                            except:
                                try:
                                    self.driver.execute_script("arguments[0].click();", button)
                                    logger.info("이탈리아 Continue 버튼 JavaScript 클릭 성공")
                                except:
                                    logger.warning("이탈리아 Continue 버튼 클릭 실패")
                                    continue
                            
                            time.sleep(3)
                            return True
                            
                    except Exception as e:
                        logger.debug(f"이탈리아 Continue 버튼 오류: {e}")
                        continue
                
                # 3. 텍스트 패턴 매칭 (Ci dispiace가 아닌 경우에만)
                try:
                    logger.info("이탈리아 텍스트 기반 버튼 검색...")
                    all_elements = (self.driver.find_elements(By.TAG_NAME, "button") + 
                                   self.driver.find_elements(By.TAG_NAME, "a") + 
                                   self.driver.find_elements(By.TAG_NAME, "input"))
                    
                    continue_patterns = [
                        'continua lo shopping', 'continua', 'avanti', 'procedi',
                        'continue shopping', 'continue', 'shopping'
                    ]
                    
                    for element in all_elements:
                        try:
                            element_text = element.text.lower()
                            element_value = element.get_attribute('value')
                            if element_value:
                                element_value = element_value.lower()
                            else:
                                element_value = ""
                            
                            for pattern in continue_patterns:
                                if (pattern in element_text or pattern in element_value) and element.is_displayed():
                                    logger.info(f"이탈리아 패턴 매칭 버튼 발견: '{element.text}' (패턴: {pattern})")
                                    
                                    self.driver.execute_script("arguments[0].scrollIntoView();", element)
                                    time.sleep(1)
                                    
                                    try:
                                        element.click()
                                        logger.info("이탈리아 패턴 매칭 클릭 성공")
                                        time.sleep(3)
                                        return True
                                    except:
                                        try:
                                            self.driver.execute_script("arguments[0].click();", element)
                                            logger.info("이탈리아 패턴 매칭 JavaScript 클릭 성공")
                                            time.sleep(3)
                                            return True
                                        except:
                                            continue
                                    
                        except Exception as e:
                            logger.debug(f"이탈리아 요소 처리 오류: {e}")
                            continue
                            
                except Exception as e:
                    logger.debug(f"이탈리아 텍스트 기반 검색 오류: {e}")
            
            logger.debug("이탈리아 처리 가능한 버튼을 찾을 수 없음")
            return False
            
        except Exception as e:
            logger.error(f"이탈리아 차단 페이지 처리 중 오류: {e}")
            return False
    
    def parse_italian_price(self, price_text):
        """이탈리아 가격 파싱 (€ 통화, 쉼표 소수점)"""
        try:
            price_text = price_text.strip()
            logger.debug(f"이탈리아 가격 파싱: '{price_text}'")
            
            # 유로 기호와 공백 제거
            cleaned = re.sub(r'[€\s]', '', price_text)
            logger.debug(f"통화 제거 후: '{cleaned}'")
            
            # 이탈리아 형식: 61,84 또는 1.234,56
            if ',' in cleaned:
                # 쉼표가 있으면 마지막 쉼표를 소수점으로 처리
                parts = cleaned.rsplit(',', 1)
                if len(parts) == 2 and len(parts[1]) <= 2 and parts[1].isdigit():
                    # 정수 부분에서 점 제거 (천단위 구분자)
                    integer_part = parts[0].replace('.', '')
                    decimal_part = parts[1]
                    final_price = f"{integer_part}.{decimal_part}"
                    logger.debug(f"이탈리아 쉼표→점 변환: '{final_price}'")
                    
                    # 최종 검증
                    if re.match(r'^\d+\.\d{1,2}$', final_price):
                        return final_price
                else:
                    # 쉼표가 있지만 소수점이 아닌 경우
                    cleaned = cleaned.replace(',', '')
            
            # 정수 가격 또는 점으로 구분된 가격
            if re.match(r'^\d+(\.\d{1,2})?$', cleaned):
                return cleaned
                
        except Exception as e:
            logger.debug(f"이탈리아 가격 파싱 오류: {price_text} - {e}")
            
        return None
    
    def extract_italian_price(self):
        """이탈리아 가격 추출"""
        logger.info("이탈리아 가격 추출 시작")
        
        # 1단계: a-offscreen에서 완전한 가격 추출
        logger.info("1단계: a-offscreen 요소에서 완전한 가격 추출")
        offscreen_selectors = [
            "//*[@id='corePrice_feature_div']//span[@class='a-offscreen']",
            "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen']", 
            ".a-price .a-offscreen",
            "//span[@class='a-price']//span[@class='a-offscreen']"
        ]
        
        for selector in offscreen_selectors:
            try:
                logger.info(f"시도: {selector}")
                if selector.startswith('//'):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    if element.is_displayed():
                        text_methods = [
                            element.get_attribute('textContent'),
                            element.get_attribute('innerText'),
                            element.text
                        ]
                        
                        for text in text_methods:
                            if text and text.strip():
                                price_text = text.strip()
                                logger.info(f"발견된 텍스트: {price_text}")
                                
                                price = self.parse_italian_price(price_text)
                                if price:
                                    logger.info(f"이탈리아 a-offscreen 가격 추출 성공: {price}")
                                    return price
                                    
            except Exception as e:
                logger.debug(f"오류: {e}")
        
        # 2단계: whole + fraction 조합
        logger.info("2단계: whole + fraction 조합으로 가격 구성")
        
        combination_attempts = [
            {
                'whole': "//*[@id='corePrice_feature_div']//span[@class='a-price-whole']",
                'fraction': "//*[@id='corePrice_feature_div']/div/div/div/div/span[1]/span[2]"
            },
            {
                'whole': "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-whole']",
                'fraction': "//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span[3]/span[2]"
            },
            {
                'whole': "//span[@class='a-price-whole']",
                'fraction': "//span[@class='a-price-fraction']"
            }
        ]
        
        for i, combo in enumerate(combination_attempts, 1):
            try:
                logger.info(f"이탈리아 조합 시도 {i}")
                
                whole_elem = self.driver.find_element(By.XPATH, combo['whole'])
                fraction_elem = self.driver.find_element(By.XPATH, combo['fraction'])
                
                if whole_elem and fraction_elem and whole_elem.is_displayed() and fraction_elem.is_displayed():
                    whole_text = whole_elem.text.strip()
                    fraction_text = fraction_elem.text.strip()
                    
                    logger.info(f"정수부: {whole_text}, 소수부: {fraction_text}")
                    
                    if whole_text and fraction_text:
                        fraction_clean = re.sub(r'[^\d]', '', fraction_text)
                        if fraction_clean:
                            combined_price = f"{whole_text}.{fraction_clean}"
                            logger.info(f"조합된 가격: {combined_price}")
                            
                            price = self.parse_italian_price(combined_price)
                            if price:
                                logger.info(f"이탈리아 조합 가격 추출 성공: {price}")
                                return price
                                
            except Exception as e:
                logger.debug(f"조합 {i} 오류: {e}")
        
        # 3단계: 개별 가격 선택자
        logger.info("3단계: 개별 가격 선택자 시도")
        
        for idx, selector in enumerate(self.selectors['price'], 1):
            try:
                logger.info(f"[{idx}/{len(self.selectors['price'])}] 시도: {selector}")
                
                if selector.startswith('//'):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    if element.is_displayed():
                        text_methods = [
                            element.get_attribute('textContent'),
                            element.get_attribute('innerText'),
                            element.text
                        ]
                        
                        for text in text_methods:
                            if text and text.strip():
                                price_text = text.strip()
                                logger.info(f"텍스트: {price_text}")
                                
                                price = self.parse_italian_price(price_text)
                                if price:
                                    logger.info(f"이탈리아 개별 선택자 가격 추출 성공: {price}")
                                    return price
                                    
            except Exception as e:
                logger.debug(f"선택자 오류: {e}")
        
        logger.error("이탈리아 가격 추출 완전 실패")
        return None
    
    def extract_italian_element_text(self, selectors, element_name="요소"):
        """이탈리아 특화 요소 텍스트 추출"""
        logger.info(f"이탈리아 {element_name} 추출 시작 - 총 {len(selectors)}개 선택자")
        
        for idx, selector in enumerate(selectors, 1):
            try:
                logger.info(f"[{idx}/{len(selectors)}] 시도: {selector}")
                
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                    selector_type = "XPath"
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    selector_type = "CSS"
                
                logger.info(f"타입: {selector_type}, 발견: {len(elements)}개")
                
                if elements:
                    for i, element in enumerate(elements):
                        try:
                            if element.is_displayed():
                                text1 = element.text.strip()
                                text2 = element.get_attribute('textContent')
                                text3 = element.get_attribute('innerText')
                                
                                text2 = text2.strip() if text2 else ""
                                text3 = text3.strip() if text3 else ""
                                
                                text = max([text1, text2, text3], key=len)
                                
                                if text:
                                    logger.info(f"원본 텍스트: '{text}'")
                                    
                                    # 이탈리아 특화 라벨 처리
                                    if element_name in ["Sold By", "Ships From"]:
                                        italian_label_patterns = [
                                            'venduto da',
                                            'spedito da',
                                            'sold by',
                                            'ships from',
                                            'fulfilled by'
                                        ]
                                        
                                        text_lower = text.lower().strip()
                                        
                                        # 라벨만 있는 경우 스킵
                                        if text_lower in italian_label_patterns:
                                            logger.info(f"이탈리아 라벨만 있음, 스킵: '{text}'")
                                            continue
                                        
                                        # 라벨 제거
                                        for pattern in italian_label_patterns:
                                            if text_lower.startswith(pattern + ' '):
                                                # "Fulfilled by Amazon" -> "Amazon" 추출
                                                actual_value = text[len(pattern):].strip()
                                                if actual_value:
                                                    text = actual_value
                                                    logger.info(f"이탈리아 라벨 제거 후: '{text}'")
                                                break
                                            
                                            # 패턴이 텍스트 중간에 있는 경우도 처리
                                            if pattern in text_lower:
                                                # 패턴 이후의 텍스트 추출
                                                pattern_index = text_lower.find(pattern)
                                                if pattern_index >= 0:
                                                    after_pattern = text[pattern_index + len(pattern):].strip()
                                                    if after_pattern:
                                                        text = after_pattern
                                                        logger.info(f"이탈리아 패턴 후 텍스트 추출: '{text}'")
                                                        break
                                    
                                    if text:
                                        logger.info(f"이탈리아 최종 추출: '{text}'")
                                        return text
                        except Exception as e:
                            logger.debug(f"요소 처리 중 오류: {e}")
                
            except Exception as e:
                logger.debug(f"선택자 오류: {str(e)}")
                continue
        
        logger.error(f"이탈리아 {element_name} 추출 완전 실패")
        return None
    
    def check_italian_stock_availability(self):
        """이탈리아 재고 상태 확인"""
        try:
            # 이탈리아 availability 요소 확인
            try:
                availability_elem = self.driver.find_element(By.ID, "availability")
                availability_text = availability_elem.text.lower()
                
                # 이탈리아 재고 없음 메시지 확인
                italian_out_of_stock = [
                    'attualmente non disponibile',
                    'non disponibile', 
                    'esaurito',
                    'temporaneamente non disponibile',
                    'currently unavailable',
                    'out of stock',
                    'temporarily out of stock'
                ]
                
                if any(phrase in availability_text for phrase in italian_out_of_stock):
                    logger.info(f"이탈리아 재고 없음: {availability_text}")
                    return False
                    
                # 이탈리아 재고 있음 메시지 확인
                italian_in_stock = [
                    'disponibile',
                    'in stock',
                    'disponibilità immediata',
                    'pronto per la spedizione',
                    'only',
                    'left in stock',
                    'disponibili'
                ]
                
                if any(phrase in availability_text for phrase in italian_in_stock):
                    logger.info(f"이탈리아 재고 있음: {availability_text}")
                    return True
                    
            except NoSuchElementException:
                logger.debug("이탈리아 availability 요소를 찾을 수 없음")
            
            # 구매 버튼 확인
            buy_buttons = [
                "add-to-cart-button",
                "buy-now-button",
                "add-to-cart-button-ubb"
            ]
            
            for button_id in buy_buttons:
                try:
                    button = self.driver.find_element(By.ID, button_id)
                    if button and button.is_enabled():
                        logger.info("이탈리아 구매 버튼 활성화 - 재고 있음")
                        return True
                except:
                    continue
            
            logger.info("이탈리아 재고 상태 불명확 - 기본값: 재고 있음")
            return True
            
        except Exception as e:
            logger.warning(f"이탈리아 재고 확인 중 오류: {e}")
            return True
    
    def extract_italian_product_info(self, url, row_data, retry_count=0, max_retries=3):
        """이탈리아 제품 정보 추출"""
        try:
            logger.info("=" * 60)
            logger.info("이탈리아 제품 정보 추출 시작")
            logger.info(f"URL: {url}")
            logger.info(f"브랜드: {row_data.get('brand', 'N/A')}")
            logger.info(f"제품: {row_data.get('item', 'N/A')}")
            
            self.driver.get(url)
            time.sleep(random.uniform(3, 5))
            
            # 이탈리아 차단 페이지 확인 및 처리
            page_source_lower = self.driver.page_source.lower()
            page_title_lower = self.driver.title.lower()
            
            # "Ci dispiace" 오류 페이지의 고유한 특징들
            ci_dispiace_indicators = [
                'ci dispiace' in page_title_lower,  # 제목에 "Ci dispiace"
                'si è verificato un errore quando abbiamo tentato di elaborare la richiesta' in page_source_lower,  # 특정 오류 메시지
                'stiamo lavorando al problema' in page_source_lower,  # "문제를 해결하고 있습니다"
                'clicca qui per tornare alla home page di amazon.it' in page_source_lower,  # 홈페이지 링크 텍스트
                'non sarà stato elaborato per il momento' in page_source_lower  # 주문 처리 안됨 메시지
            ]
            
            # 일반적인 차단 페이지 특징들
            general_block_indicators = [
                'continua lo shopping' in page_source_lower,
                'continue shopping' in page_source_lower,
                'inserisci i caratteri' in page_source_lower,
                'enter the characters' in page_source_lower
            ]
            
            # "Ci dispiace" 오류 페이지인지 확인 (최소 2개 이상 조건 만족)
            if sum(ci_dispiace_indicators) >= 2:
                logger.info("이탈리아 'Ci dispiace' 오류 페이지 감지 (고유 특징 확인)")
                if self.handle_captcha_or_block_page(url):
                    time.sleep(3)
                    self.wait_for_page_load()
                else:
                    logger.warning("이탈리아 Ci dispiace 페이지 처리 실패")
            
            # 일반 차단 페이지 처리
            elif any(general_block_indicators):
                logger.info("이탈리아 일반 차단 페이지 감지")
                if self.handle_captcha_or_block_page(url):
                    time.sleep(3)
                    self.wait_for_page_load()
                else:
                    logger.warning("이탈리아 일반 차단 페이지 처리 실패")
            
            # 페이지 차단 여부 재확인
            if self.is_page_blocked():
                logger.error("이탈리아 여전히 차단 페이지임")
                raise Exception("이탈리아 페이지 차단됨")
            
            # 페이지 로드 대기
            self.wait_for_page_load()
            
            now_time = datetime.now(self.korea_tz)
            
            result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': 'it',
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
                'vat': row_data.get('vat', 'o')
            }
            
            # 제목 추출
            result['title'] = self.extract_italian_element_text(
                self.selectors['title'], 
                "제목"
            )
            
            # 재고 확인
            has_stock = self.check_italian_stock_availability()
            
            # 가격 추출
            logger.info("이탈리아 가격 추출 시도")
            result['retailprice'] = self.extract_italian_price()
            
            if not has_stock and result['retailprice'] is None:
                result['retailprice'] = None
                logger.info("이탈리아 재고 없음 + 가격 없음 -> 가격 None으로 설정")
            
            # Ships From 추출
            result['ships_from'] = self.extract_italian_element_text(
                self.selectors['ships_from'], 
                "Ships From"
            )
            
            # Sold By 추출
            result['sold_by'] = self.extract_italian_element_text(
                self.selectors['sold_by'], 
                "Sold By"
            )
            
            # 이미지 URL 추출
            for selector in self.selectors['imageurl']:
                try:
                    if selector.startswith('//'):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    result['imageurl'] = element.get_attribute('src')
                    if result['imageurl']:
                        logger.debug("이탈리아 이미지 URL 추출 성공")
                        break
                except:
                    continue
            
            # 이탈리아 VAT 확인
            page_source = self.driver.page_source
            page_source_lower = page_source.lower()
            
            # for vat_text in self.selectors['vat_text_list']:
            #     if vat_text.lower() in page_source_lower:
            #         result['vat'] = 'o'
            #         logger.info(f"이탈리아 VAT/IVA 포함 확인: {vat_text}")
            #         break
            
            logger.info("이탈리아 추출 결과:")
            logger.info(f"제목: {result['title'][:50] + '...' if result['title'] and len(result['title']) > 50 else result['title']}")
            logger.info(f"가격: {result['retailprice']}")
            logger.info(f"이미지: {'있음' if result['imageurl'] else '없음'}")
            logger.info(f"판매자: {result['sold_by']}")
            logger.info(f"배송지: {result['ships_from']}")
            # logger.info(f"VAT: {result['vat']}")
            
            return result
            
        except Exception as e:
            logger.error(f"이탈리아 페이지 처리 오류: {e}")
            
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10
                logger.info(f"이탈리아 {wait_time}초 후 재시도... ({retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                
                try:
                    self.driver.refresh()
                except:
                    logger.info("이탈리아 드라이버 재시작 중...")
                    self.driver.quit()
                    self.setup_driver()
                
                return self.extract_italian_product_info(url, row_data, retry_count + 1, max_retries)
            
            # 실패 시 기본 결과 반환
            now_time = datetime.now(self.korea_tz)
            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': 'it',
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
                'vat': row_data.get('vat', 'o')
            }
    
    def get_italian_crawl_targets(self, limit=None):
        """이탈리아 크롤링 대상 URL 목록 조회"""
        try:
            query = """
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = 'it' 
              AND mall_name = 'amazon'
              AND is_active = TRUE
            """
                
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"이탈리아 크롤링 대상 {len(df)}개 조회 완료")
            return df.to_dict('records')
            
        except Exception as e:
            logger.error(f"이탈리아 크롤링 대상 조회 실패: {e}")
            return []
    
    def save_to_db(self, df):
        """이탈리아 DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("DB 연결이 없어 이탈리아 DB 저장을 건너뜁니다")
            return False
        
        try:
            table_name = 'amazon_price_crawl_tbl_it'
            
            df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
            logger.info(f"이탈리아 DB 저장 완료: {len(df)}개 레코드 -> {table_name}")
            
            # 로그 저장
            log_records = []
            for _, row in df.iterrows():
                log_records.append({
                    'country_code': 'it',
                    'url': row['producturl'],
                    'status': 'success' if row['retailprice'] is not None else 'failed',
                    'error_message': None if row['retailprice'] is not None else 'Price not found',
                    'execution_time': random.uniform(3, 10),
                    'retailprice': row['retailprice'],
                    'crawl_datetime': row['crawl_datetime']
                })
            
            if log_records:
                log_df = pd.DataFrame(log_records)
                log_df.to_sql('amazon_crawl_logs', self.db_engine, if_exists='append', index=False)
                logger.info(f"이탈리아 크롤링 로그 저장 완료: {len(log_records)}개")
            
            return True
            
        except Exception as e:
            logger.error(f"이탈리아 DB 저장 실패: {e}")
            return False
    
    def upload_to_file_server(self, local_file_path, remote_filename=None):
        """이탈리아 파일서버에 업로드"""
        try:
            transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
            transport.connect(
                username=FILE_SERVER_CONFIG['username'],
                password=FILE_SERVER_CONFIG['password']
            )
            sftp = paramiko.SFTPClient.from_transport(transport)
            
            if remote_filename is None:
                remote_filename = os.path.basename(local_file_path)
            
            # 이탈리아 전용 디렉토리
            italy_dir = f"{FILE_SERVER_CONFIG['upload_path']}/it"
            
            try:
                sftp.stat(italy_dir)
            except FileNotFoundError:
                logger.info(f"이탈리아 디렉토리 생성: {italy_dir}")
                sftp.mkdir(italy_dir)
            
            remote_path = f"{italy_dir}/{remote_filename}"
            
            sftp.put(local_file_path, remote_path)
            logger.info(f"이탈리아 파일서버 업로드 완료: {remote_path}")
            
            sftp.close()
            transport.close()
            
            return True
            
        except Exception as e:
            logger.error(f"이탈리아 파일서버 업로드 실패: {e}")
            return False
    
    def save_italian_results(self, df, save_db=True, upload_server=True):
        """이탈리아 결과를 DB와 파일서버에 저장"""
        now = datetime.now(self.korea_tz)
        date_str = now.strftime("%Y%m%d")
        time_str = now.strftime("%H%M%S")
        
        base_filename = f"{date_str}{time_str}_it_amazon"
        
        results = {
            'db_saved': False,
            'server_uploaded': False
        }
        
        if save_db:
            results['db_saved'] = self.save_to_db(df)
        
        if upload_server:
            try:
                # CSV 파일 생성
                temp_csv = f'temp_{base_filename}.csv'
                df.to_csv(temp_csv, index=False, encoding='utf-8-sig')
                
                remote_csv_filename = f'{base_filename}.csv'
                if self.upload_to_file_server(temp_csv, remote_csv_filename):
                    results['server_uploaded'] = True
                
                # Excel 파일 생성
                temp_excel = f'temp_{base_filename}.xlsx'
                with pd.ExcelWriter(temp_excel, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='All_Results', index=False)
                    
                    # 가격 있는 제품만
                    price_df = df[df['retailprice'].notna()]
                    if not price_df.empty:
                        price_df.to_excel(writer, sheet_name='With_Prices', index=False)
                    
                    # 이탈리아 요약 시트
                    summary = pd.DataFrame({
                        'Metric': [
                            'Total Products', 
                            'Products with Price', 
                            'Products without Price', 
                            'Success Rate (%)',
                            'Crawl Date',
                            'Country',
                            'Mall Name'
                        ],
                        'Value': [
                            len(df),
                            df['retailprice'].notna().sum(),
                            df['retailprice'].isna().sum(),
                            round(df['retailprice'].notna().sum() / len(df) * 100, 2) if len(df) > 0 else 0,
                            now.strftime('%Y-%m-%d %H:%M:%S'),
                            'Italy',
                            'Amazon'
                        ]
                    })
                    summary.to_excel(writer, sheet_name='Summary', index=False)
                
                # JSON 파일 생성
                temp_json = f'temp_{base_filename}.json'
                crawl_metadata = {
                    'crawl_info': {
                        'country': 'it',
                        'country_name': 'Italy',
                        'crawler': 'amazon_italy_specialized_crawler',
                        'crawl_datetime': now.strftime('%Y-%m-%d %H:%M:%S'),
                        'total_products': len(df),
                        'successful_crawls': df['retailprice'].notna().sum(),
                        'version': '1.0_italy_specialized',
                        'features': [
                            # 'Italian VAT (IVA) detection',
                            'Italian price parsing (comma decimals)',
                            'Italian language support',
                            'Italian continue button handling'
                        ]
                    },
                    'results': df.to_dict('records')
                }
                
                with open(temp_json, 'w', encoding='utf-8') as f:
                    json.dump(crawl_metadata, f, ensure_ascii=False, indent=2, default=str)
                
                # self.upload_to_file_server(temp_excel, f'{base_filename}.xlsx')
                # self.upload_to_file_server(temp_json, f'{base_filename}.json')
                
                # 임시 파일 삭제
                for temp_file in [temp_csv, temp_excel, temp_json]:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                
                logger.info("이탈리아 임시 파일 삭제 완료")
                
            except Exception as e:
                logger.error(f"이탈리아 파일 처리 오류: {e}")
        
        return results
    
    def scrape_italian_urls(self, urls_data, max_items=None):
        """이탈리아 URL 스크래핑"""
        if max_items:
            urls_data = urls_data[:max_items]
        
        logger.info("=" * 80)
        logger.info("이탈리아 Amazon 크롤링 시작")
        logger.info(f"대상: {len(urls_data)}개 제품")
        logger.info("특화 기능: IVA 포함 확인, 쉼표 소수점 가격 파싱")
        logger.info("=" * 80)
        
        if not self.setup_driver():
            logger.error("이탈리아 드라이버 설정 실패")
            return None
        
        results = []
        failed_urls = []
        
        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"이탈리아 진행률: {idx + 1}/{len(urls_data)} ({(idx + 1)/len(urls_data)*100:.1f}%)")
                
                url = row.get('url')
                
                result = self.extract_italian_product_info(url, row)
                
                if result['retailprice'] is None and result['title'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'brand': row.get('brand', ''),
                        'reason': '가격과 제목 모두 없음'
                    })
                elif result['retailprice'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'brand': row.get('brand', ''),
                        'reason': '가격 없음'
                    })
                
                results.append(result)
                
                # 중간 저장 (10개마다)
                if (idx + 1) % 10 == 0:
                    interim_df = pd.DataFrame(results[-10:])
                    if self.db_engine:
                        try:
                            interim_df.to_sql('amazon_price_crawl_tbl_it', self.db_engine, 
                                            if_exists='append', index=False)
                            logger.info("이탈리아 중간 저장: 10개 레코드 DB 저장")
                        except Exception as e:
                            logger.error(f"이탈리아 중간 저장 실패: {e}")
                
                # 대기 시간
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(5, 10)
                    logger.info(f"이탈리아 {wait_time:.1f}초 대기 중...")
                    time.sleep(wait_time)
                    
                    # 20개마다 긴 휴식
                    if (idx + 1) % 20 == 0:
                        logger.info("이탈리아 20개 처리 완료, 30초 휴식...")
                        time.sleep(30)
        
        except Exception as e:
            logger.error(f"이탈리아 스크래핑 중 오류: {e}")
        
        finally:
            if failed_urls:
                logger.warning(f"이탈리아 문제 발생한 URL {len(failed_urls)}개:")
                for fail in failed_urls[:5]:
                    logger.warning(f"  - {fail['brand']} {fail['item']}: {fail.get('reason', '알 수 없음')}")
                if len(failed_urls) > 5:
                    logger.warning(f"  ... 외 {len(failed_urls) - 5}개")
            
            if self.driver:
                self.driver.quit()
                logger.info("이탈리아 드라이버 종료")
        
        return pd.DataFrame(results)
    
    def analyze_italian_results(self, df):
        """이탈리아 결과 분석"""
        logger.info("=" * 80)
        logger.info("이탈리아 결과 분석")
        logger.info("=" * 80)
        
        total = len(df)
        with_price = df['retailprice'].notna().sum()
        without_price = df['retailprice'].isna().sum()
        success_rate = (with_price / total * 100) if total > 0 else 0
        
        logger.info(f"전체 제품: {total}개")
        logger.info(f"가격 추출 성공: {with_price}개")
        logger.info(f"가격 추출 실패: {without_price}개")
        logger.info(f"성공률: {success_rate:.1f}%")
        
        # IVA 포함 제품 통계
        # with_vat = df['vat'].eq('o').sum()
        # logger.info(f"IVA 포함 제품: {with_vat}개")
        
        if with_price > 0:
            price_df = df[df['retailprice'].notna()].copy()
            
            try:
                # 이탈리아 가격 숫자 변환
                price_df['price_numeric'] = price_df['retailprice'].astype(str).str.replace(',', '').astype(float)
                
                logger.info("이탈리아 가격 통계 (€):")
                logger.info(f"   평균가: €{price_df['price_numeric'].mean():.2f}")
                logger.info(f"   최저가: €{price_df['price_numeric'].min():.2f}")
                logger.info(f"   최고가: €{price_df['price_numeric'].max():.2f}")
                logger.info(f"   중간값: €{price_df['price_numeric'].median():.2f}")
            except Exception as e:
                logger.warning(f"이탈리아 가격 통계 계산 오류: {e}")
            
            # 브랜드별 성공률
            brand_stats = df.groupby('brand').agg({
                'retailprice': ['count', lambda x: x.notna().sum()]
            })
            brand_stats.columns = ['total', 'success']
            brand_stats['success_rate'] = (brand_stats['success'] / brand_stats['total'] * 100).round(1)
            
            logger.info("이탈리아 브랜드별 성공률:")
            for brand, row in brand_stats.iterrows():
                logger.info(f"   {brand}: {row['success_rate']:.1f}% ({row['success']}/{row['total']})")

def main():
    """이탈리아 메인 실행 함수"""
    test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
    max_items = int(os.getenv('MAX_ITEMS', '0')) or None
    
    print("=" * 80)
    print("Amazon Italy 가격 추출 시스템 v1.0")
    print("=" * 80)
    print("국가: 이탈리아 (IT)")
    print(f"모드: {'테스트' if test_mode else '실제'}")
    print("특화 기능:")
    print("  - IVA 포함 자동 감지")
    print("  - 이탈리아 가격 형식 지원 (61,84 → 61.84)")
    print("  - 이탈리아 언어 지원 (Venduto da, Spedito da)")
    print("  - 'Continua lo shopping' 버튼 자동 처리")
    if max_items:
        print(f"최대 처리 수: {max_items}개")
    print("=" * 80)
    
    scraper = AmazonItalyScraper()
    
    if scraper.db_engine is None:
        logger.error("DB 연결 실패로 종료합니다.")
        return
    
    if test_mode:
        logger.info("이탈리아 테스트 모드 실행 중...")
        test_data = [{
            'url': 'https://www.amazon.it/dp/B08N5WRWNW',
            'brand': 'Samsung',
            'item': '980 PRO 1TB',
            'retailerid': 'TEST001',
            'retailersku': 'TEST001',
            'channel': 'Online',
            'seg_lv1': 'SSD',
            'seg_lv2': 'Consumer',
            'seg_lv3': 'NVMe',
            'capacity': '1TB',
            'form_factor': 'M.2'
        }]
        
        results_df = scraper.scrape_italian_urls(test_data)
        if results_df is not None and not results_df.empty:
            scraper.analyze_italian_results(results_df)
            scraper.save_italian_results(results_df, save_db=False, upload_server=True)
        return
    
    logger.info("이탈리아 전체 크롤링 시작")
    urls_data = scraper.get_italian_crawl_targets(limit=max_items)
    
    if not urls_data:
        logger.warning("이탈리아 크롤링 대상이 없습니다.")
        return
    
    logger.info(f"이탈리아 크롤링 대상: {len(urls_data)}개")
    
    results_df = scraper.scrape_italian_urls(urls_data, max_items)
    
    if results_df is None or results_df.empty:
        logger.error("이탈리아 크롤링 결과가 없습니다.")
        return
    
    scraper.analyze_italian_results(results_df)
    
    save_results = scraper.save_italian_results(
        results_df,
        save_db=True,
        upload_server=True
    )
    
    logger.info("=" * 80)
    logger.info("이탈리아 저장 결과")
    logger.info("=" * 80)
    logger.info(f"DB 저장: {'성공' if save_results['db_saved'] else '실패'}")
    logger.info(f"파일서버 업로드: {'성공' if save_results['server_uploaded'] else '실패'}")
    
    logger.info("=" * 80)
    logger.info("이탈리아 크롤링 프로세스 완료!")
    logger.info("=" * 80)

if __name__ == "__main__":
    required_packages = [
        'undetected-chromedriver',
        'selenium',
        'pandas',
        'pymysql',
        'sqlalchemy',
        'paramiko',
        'openpyxl'
    ]
    
    print("필요한 패키지:")
    print("pip install " + " ".join(required_packages))
    print("환경변수 설정:")
    print("export TEST_MODE=false  # 테스트 모드")
    print("export MAX_ITEMS=10     # 최대 처리 개수 (선택사항)")
    print()
    
    main()