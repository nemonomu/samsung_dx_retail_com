def is_excluded_price_element(self, element):
        """가격 요소가 제외 대상인지 확인"""
        try:
            element_html = self.driver.execute_script("return arguments[0].outerHTML;", element)
            excluded_areas = self.selectors[self.country_code].get('excluded_price_areas', [])
            
            for pattern in excluded_areas:
                if pattern in element_html.lower():
                    logger.debug(f"제외 영역 감지: {pattern}")
                    return True
            
            parent_checks = [
                "arguments[0].closest('.product-comparison-desktop')",
                "arguments[0].closest('[class*=\"comparison\"]')",
                "arguments[0].closest('[class*=\"non-deal\"]')"
            ]
            
            for check in parent_checks:
                try:
                    parent = self.driver.execute_script(f"return {check}", element)
                    if parent:
                        logger.debug(f"제외 상위 요소 발견")
                        return True
                except:
                    continue
            
            return False
        except Exception as e:
            logger.debug(f"제외 요소 확인 오류: {e}")
            return False# -*- coding: utf-8 -*-
"""
Amazon 가격 추출 시스템 - UK/DE/ES 통합 문제 해결 완전판
WinError 6 오류만 수정
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

class AmazonScraper:
    def __init__(self, country_code='usa'):
        self.driver = None
        self.db_engine = None
        self.country_code = country_code.lower()
        self.wait = None
        self.korea_tz = pytz.timezone('Asia/Seoul')
        
        self.setup_db_connection()
        self.setup_enhanced_selectors()
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
    
    def setup_enhanced_selectors(self):
        """강화된 선택자 설정"""
        self.selectors = {
            self.country_code: {
                'price': [
                    # DE 전용 가격 선택자
                    "//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span[1]",
                    "/html/body/div[2]/div/div/div[4]/div[4]/div[13]/div/div/div[3]/div[1]/span[1]",
                    
                    # 추가된 선택자들
                    "//*[@id='usedBuySection']/div[1]/div/span[2]",
                    "/html/body/div[2]/div/div/div[4]/div[1]/div[4]/div/div[1]/div/div/div/form/div/div[1]/div/div/div/div[1]/div[1]/div/span[2]",
                    "//*[@id='corePrice_feature_div']/div/div/div/div/span[1]/span[1]",
                    
                    # 메인 가격 영역
                    "//*[@id='corePrice_feature_div']//span[@class='a-offscreen']",
                    "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-offscreen']",
                    "//*[@id='apex_desktop']//span[@class='a-price']//span[@class='a-offscreen']",
                    
                    # 첫 번째 가격만
                    "(//span[@class='a-price']//span[@class='a-offscreen'])[1]",
                    "(//span[@class='a-price-whole'])[1]",
                    
                    # 기본 가격 요소들
                    "//*[@id='priceblock_ourprice']",
                    "//*[@id='priceblock_dealprice']",
                    "//*[@id='listPrice']",
                    
                    # whole 가격
                    "//*[@id='corePrice_feature_div']//span[@class='a-price-whole']",
                    "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-whole']",
                    "//*[@id='apex_desktop']//span[@class='a-price-whole']",
                    
                    # 추가 선택자들
                    "//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span[2]/span[2]",
                    "//*[@id='usedBuySection']/div[1]/div"
                ],
                'price_fraction': [
                    "//*[@id='corePrice_feature_div']//span[@class='a-price-fraction']",
                    "//*[@id='corePriceDisplay_desktop_feature_div']//span[@class='a-price-fraction']",
                    "//*[@id='apex_desktop']//span[@class='a-price-fraction']",
                    "//span[@class='a-price-fraction']"
                ],
                'title': [
                    "#productTitle",
                    "//span[@id='productTitle']",
                    "//h1/span[@id='productTitle']"
                ],
                'ships_from': [
                    "//*[@id='fulfillerInfoFeature_feature_div']/div[2]/div[1]/span",
                    "//div[@id='fulfillerInfoFeature_feature_div']//span",
                    "//a[@id='SSOFpopoverLink_ubb']"
                ],
                'sold_by': [
                    
                    "//*[@id='merchantInfoFeature_feature_div']/div[2]/div[1]/span",
                    "//*[@id='sellerProfileTriggerId']"
                    
                  
                ],
                'imageurl': [
                    "//div[@id='imageBlock']//img[@id='landingImage']",
                    "//div[@id='main-image-container']//img",
                    "//img[@class='a-dynamic-image']"
                ],
                'availability': [
                    "//div[@id='availability']//span",
                    "//div[@id='availability_feature_div']//span"
                # ],
                # 'vat_text_list': [
                #     "Tax included", 
                #     "include VAT.",
                #     "VAT included", 
                #     "inkl. MwSt", 
                #     "TVA incluse", 
                #     "IVA incluida",
                ],
                'excluded_price_areas': [
                    'product-comparison',
                    'comparison-desktop',
                    'non-deal-price',
                    'strikethrough',
                    'list-price',
                    'rrp-price',
                    'was-price',
                    'capacity-selection'
                ]
            }
        }
    
    def load_selectors_from_db(self):
        """DB에서 선택자 로드"""
        if not self.db_engine:
            logger.warning("DB 연결이 없어 선택자 로드 불가")
            return
        
        try:
            query = """
            SELECT element_type, selector_value, priority
            FROM amazon_selectors
            WHERE country_code = %s 
              AND is_active = TRUE
            ORDER BY element_type, priority ASC
            """
            
            df = pd.read_sql(query, self.db_engine, params=(self.country_code,))
            logger.info(f"DB에서 선택자 로드: {len(df)}개")
            
            if len(df) > 0:
                for element_type in df['element_type'].unique():
                    selectors = df[df['element_type'] == element_type]['selector_value'].tolist()
                    if element_type in self.selectors[self.country_code]:
                        existing = self.selectors[self.country_code][element_type]
                        self.selectors[self.country_code][element_type] = existing + selectors
                    else:
                        self.selectors[self.country_code][element_type] = selectors
                
                logger.info("DB 선택자 병합 완료")
        except Exception as e:
            logger.error(f"DB 선택자 로드 실패: {e}")
    
    def setup_driver(self):
        """Chrome 드라이버 설정"""
        logger.info("Chrome 드라이버 설정 중...")
        
        try:
            options = uc.ChromeOptions()
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-setuid-sandbox')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-software-rasterizer')
            
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ]
            options.add_argument(f'--user-agent={random.choice(user_agents)}')
            
            language_map = {
                'usa': 'en-US,en',
                'gb': 'en-GB,en',
                'de': 'de-DE,de',
                'fr': 'fr-FR,fr',
                'es': 'es-ES,es',
                'it': 'it-IT,it',
                'jp': 'ja-JP,ja',
                'in': 'en-IN,en'
            }
            lang = language_map.get(self.country_code, 'en-US,en')
            options.add_experimental_option('prefs', {'intl.accept_languages': lang})
            
            self.driver = uc.Chrome(options=options)
            self.driver.maximize_window()
            self.wait = WebDriverWait(self.driver, 20)
            
            logger.info("드라이버 설정 완료")
            return True
        except Exception as e:
            logger.error(f"드라이버 설정 실패: {e}")
            return False
    
    def handle_captcha_or_block_page(self):
        """차단 페이지나 캡차 처리"""
        try:
            logger.info("차단/캡차 페이지 확인 중...")
            
            continue_button_texts = {
                'es': ['Seguir comprando', 'Continuar comprando'],
                'de': ['Weiter shoppen', 'Weiter einkaufen'],
                'fr': ['Continuer les achats', 'Continuer', 'Continuer les achats'],
                'it': ['Continua lo shopping', 'Continua'],
                'gb': ['Continue shopping', 'Continue'],
                'usa': ['Continue shopping', 'Continue']
            }
            
            texts = continue_button_texts.get(self.country_code, ['Continue shopping', 'Continue'])
            
            all_selectors = []
            for text in texts:
                all_selectors.extend([
                    f"//button[contains(text(), '{text}')]",
                    f"//input[@value='{text}']",
                    f"//a[contains(text(), '{text}')]",
                    f"//span[contains(text(), '{text}')]/ancestor::button"
                ])
            
            all_selectors.extend([
                "//button[contains(@class, 'a-button-primary')]",
                "button.a-button-primary",
                "button[type='submit']"
            ])
            
            for selector in all_selectors:
                try:
                    if selector.startswith('//'):
                        button = self.driver.find_element(By.XPATH, selector)
                    else:
                        button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if button and button.is_displayed():
                        button_text = button.text
                        logger.info(f"Continue 버튼 발견: {button_text}")
                        
                        try:
                            button.click()
                            logger.info("버튼 클릭 성공")
                        except:
                            self.driver.execute_script("arguments[0].click();", button)
                            logger.info("JavaScript 클릭 성공")
                        
                        time.sleep(3)
                        return True
                except Exception as e:
                    continue
            
            return False
        except Exception as e:
            logger.error(f"차단 페이지 처리 중 오류: {e}")
            return False
    
    def is_excluded_price_element(self, element):
        """가격 요소가 제외 대상인지 확인"""
        try:
            element_html = self.driver.execute_script("return arguments[0].outerHTML;", element)
            excluded_areas = self.selectors[self.country_code].get('excluded_price_areas', [])
            
            for pattern in excluded_areas:
                if pattern in element_html.lower():
                    logger.debug(f"제외 영역 감지: {pattern}")
                    return True
            
            parent_checks = [
                "arguments[0].closest('.product-comparison-desktop')",
                "arguments[0].closest('[class*=\"comparison\"]')",
                "arguments[0].closest('[class*=\"non-deal\"]')"
            ]
            
            for check in parent_checks:
                try:
                    parent = self.driver.execute_script(f"return {check}", element)
                    if parent:
                        logger.debug(f"제외 상위 요소 발견")
                        return True
                except:
                    continue
            
            return False
        except Exception as e:
            logger.debug(f"제외 요소 확인 오류: {e}")
            return False
    
    def is_page_blocked(self):
        """페이지 차단 감지"""
        try:
            page_title = self.driver.title.lower()
            page_source = self.driver.page_source.lower()
            current_url = self.driver.current_url.lower()
            
            if '503' in page_title or 'access denied' in page_title:
                return True
            
            if 'amazon' not in current_url:
                return True
            
            return False
        except Exception as e:
            logger.error(f"페이지 차단 확인 중 오류: {e}")
            return False
    
    def wait_for_page_load(self, timeout=10):
        """페이지 로드 대기"""
        try:
            self.wait.until(lambda driver: driver.execute_script("return document.readyState") == "complete")
            return True
        except Exception as e:
            logger.warning(f"페이지 로드 대기 중 오류: {e}")
            return False
    
    def extract_clean_text_from_element(self, element, element_name="요소"):
        """요소에서 깨끗한 텍스트 추출"""
        try:
            text_methods = [
                element.get_attribute('textContent'),
                element.get_attribute('innerText'),
                element.text
            ]
            
            for text in text_methods:
                if text and text.strip():
                    clean_text = text.strip()
                    
                    if element_name in ["Ships From", "Sold By"]:
                        clean_text = self.clean_ships_sold_text(clean_text, element_name)
                    
                    if clean_text:
                        return clean_text
            
            return None
        except Exception as e:
            logger.debug(f"텍스트 추출 오류: {e}")
            return None
    
    def clean_ships_sold_text(self, text, element_type):
        """Ships From / Sold By 텍스트 정리"""
        try:
            label_patterns = {
                'Ships From': ['ships from', 'fulfilled by', 'versand durch', 'expédié par'],
                'Sold By': ['sold by', 'verkauft von', 'vendu par']
            }
            
            text_lower = text.lower().strip()
            patterns = label_patterns.get(element_type, [])
            
            if text_lower in patterns:
                return None
            
            for pattern in patterns:
                if text_lower.startswith(pattern + ' '):
                    actual_value = text[len(pattern):].strip()
                    if actual_value:
                        return actual_value
                
                if pattern in text_lower and 'amazon' in text_lower:
                    return 'Amazon'
            
            return text
        except Exception as e:
            logger.debug(f"텍스트 정리 오류: {e}")
            return text
    
    def extract_element_text(self, selectors, element_name="요소"):
        """선택자 목록에서 텍스트 추출"""
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
                                if element_name == "가격" and self.is_excluded_price_element(element):
                                    continue
                                
                                text = self.extract_clean_text_from_element(element, element_name)
                                if text:
                                    logger.info(f"최종 추출 성공: '{text}'")
                                    return text
                        except Exception as e:
                            logger.debug(f"요소 처리 중 오류: {e}")
            except Exception as e:
                logger.debug(f"선택자 오류: {e}")
                continue
        
        logger.error(f"{element_name} 추출 완전 실패")
        return None
    
    def parse_price_by_country(self, price_text, country_code):
        """국가별 가격 파싱"""
        try:
            price_text = price_text.strip()
            logger.debug(f"파싱할 가격 텍스트: '{price_text}'")
            
            invalid_patterns = [
                r'^[a-zA-Z\s]+$',
                r'^\d+\s*[a-zA-Z]',
                r'was\s*[€$£¥₹]',
                r'list\s*price',
                r'buy\s*used'
            ]
            
            for pattern in invalid_patterns:
                if re.search(pattern, price_text, re.IGNORECASE):
                    logger.debug(f"무효한 가격 패턴 감지: {pattern}")
                    return None
            
            if country_code in ['gb', 'usa']:
                cleaned = re.sub(r'[£$\s]', '', price_text)
                if re.match(r'^\d{1,4}(?:,\d{3})*(?:\.\d{1,2})?$', cleaned):
                    try:
                        price_value = float(cleaned.replace(',', ''))
                        if 5 <= price_value <= 50000:
                            return cleaned.replace(',', '')
                    except:
                        pass
            
            elif country_code in ['de', 'es', 'fr', 'it']:
                cleaned = re.sub(r'[€\s]', '', price_text)
                logger.debug(f"통화 제거 후: '{cleaned}'")
                
                if country_code == 'de':
                    if re.match(r'^\d{4}$', cleaned) and len(cleaned) == 4:
                        try:
                            cents = int(cleaned)
                            euros = cents / 100
                            if 10 <= euros <= 10000:
                                formatted_price = f"{euros:.2f}"
                                logger.debug(f"독일 센트→유로 변환: {cleaned} → {formatted_price}")
                                return formatted_price
                        except:
                            pass
                    
                    if ',' in cleaned and '.' not in cleaned:
                        cleaned = cleaned.replace(',', '.')
                    elif ',' in cleaned and '.' in cleaned:
                        parts = cleaned.rsplit(',', 1)
                        if len(parts) == 2 and len(parts[1]) <= 2:
                            integer_part = parts[0].replace('.', '')
                            decimal_part = parts[1]
                            cleaned = f"{integer_part}.{decimal_part}"
                
                elif country_code == 'es':
                    if ',' in cleaned and '.' not in cleaned:
                        cleaned = cleaned.replace(',', '.')
                    elif ',' in cleaned and '.' in cleaned:
                        parts = cleaned.rsplit(',', 1)
                        if len(parts) == 2 and len(parts[1]) <= 2:
                            integer_part = parts[0].replace('.', '')
                            decimal_part = parts[1]
                            cleaned = f"{integer_part}.{decimal_part}"
                
                if re.match(r'^\d+(?:\.\d{1,2})?$', cleaned):
                    try:
                        price_value = float(cleaned)
                        if 10 <= price_value <= 10000:
                            return cleaned
                    except:
                        pass
            
            elif country_code == 'jp':
                cleaned = re.sub(r'[¥￥\s]', '', price_text)
                if re.match(r'^\d+(?:,\d{3})*$', cleaned):
                    return cleaned
            
            elif country_code == 'in':
                cleaned = re.sub(r'[₹\s]', '', price_text)
                if re.match(r'^\d+(?:,\d{2,3})*(?:\.\d{1,2})?$', cleaned):
                    return cleaned
            
        except Exception as e:
            logger.debug(f"가격 파싱 오류: {e}")
        
        return None
    
    def extract_price(self, country_code):
        """가격 추출"""
        logger.info(f"가격 추출 시작 - 국가: {country_code}")
        
        price_selectors = self.selectors[country_code].get('price', [])
        
        for idx, selector in enumerate(price_selectors, 1):
            try:
                logger.info(f"[{idx}/{len(price_selectors)}] 시도: {selector}")
                
                # 특별한 usedBuySection 처리
                if selector == "//*[@id='usedBuySection']/div[1]/div":
                    elements = self.driver.find_elements(By.XPATH, selector)
                    for element in elements:
                        if element.is_displayed():
                            text = self.extract_clean_text_from_element(element, "가격")
                            if text and "achetez d'occasion" in text.lower():
                                # "Achetez d'occasion" 뒤의 가격 추출
                                parts = text.lower().split("achetez d'occasion")
                                if len(parts) > 1:
                                    price_part = parts[1].strip()
                                    price = self.parse_price_by_country(price_part, country_code)
                                    if price:
                                        logger.info(f"Achetez d'occasion 가격 추출 성공: {price}")
                                        return price
                    continue
                
                # 일반적인 선택자 처리
                if selector.startswith('//') or selector.startswith('('):
                    elements = self.driver.find_elements(By.XPATH, selector)
                else:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                
                for element in elements:
                    if element.is_displayed():
                        if self.is_excluded_price_element(element):
                            continue
                        
                        text = self.extract_clean_text_from_element(element, "가격")
                        if text:
                            price = self.parse_price_by_country(text, country_code)
                            if price:
                                logger.info(f"가격 추출 성공: {price}")
                                return price
            except Exception as e:
                logger.debug(f"선택자 오류: {e}")
        
        # whole + fraction 조합 시도
        logger.info("whole + fraction 조합 시도")
        try:
            whole_elem = self.driver.find_element(By.XPATH, f"//*[@id='corePrice_feature_div']//span[@class='a-price-whole']")
            fraction_elem = self.driver.find_element(By.XPATH, f"//*[@id='corePrice_feature_div']//span[@class='a-price-fraction']")
            
            if whole_elem and fraction_elem and whole_elem.is_displayed() and fraction_elem.is_displayed():
                whole_text = whole_elem.text.strip()
                fraction_text = fraction_elem.text.strip()
                
                if whole_text and fraction_text:
                    fraction_clean = re.sub(r'[^\d]', '', fraction_text)
                    if fraction_clean:
                        combined_price = f"{whole_text}.{fraction_clean}"
                        price = self.parse_price_by_country(combined_price, country_code)
                        if price:
                            logger.info(f"조합 가격 추출 성공: {price}")
                            return price
        except Exception as e:
            logger.debug(f"조합 오류: {e}")
        
        logger.error("모든 방법으로 가격 추출 실패")
        return None
    
    def check_stock_availability(self):
        """재고 상태 확인"""
        try:
            try:
                availability_elem = self.driver.find_element(By.ID, "availability")
                availability_text = availability_elem.text.lower()
                
                if any(phrase in availability_text for phrase in [
                    'currently unavailable', 'out of stock', 'temporarily out of stock',
                    'no disponible', 'agotado', 'nicht verfügbar', 'ausverkauft'
                ]):
                    logger.info(f"재고 없음: {availability_text}")
                    return False
                
                if any(phrase in availability_text for phrase in [
                    'in stock', 'only', 'left in stock', 'disponible', 'en stock', 'auf lager'
                ]):
                    logger.info(f"재고 있음: {availability_text}")
                    return True
            except:
                pass
            
            buy_buttons = ["add-to-cart-button", "buy-now-button"]
            for button_id in buy_buttons:
                try:
                    button = self.driver.find_element(By.ID, button_id)
                    if button and button.is_enabled():
                        return True
                except:
                    continue
            
            return True
        except Exception as e:
            logger.warning(f"재고 확인 중 오류: {e}")
            return True
    
    def extract_product_info(self, url, row_data, retry_count=0, max_retries=3):
        """제품 정보 추출"""
        try:
            logger.info("=" * 60)
            logger.info(f"제품 정보 추출 시작: {url}")
            
            self.driver.get(url)
            time.sleep(random.uniform(2, 4))
            
            # 프랑스 아마존 Continue 버튼 처리 (특정 문구가 있을 때만)
            try:
                page_source = self.driver.page_source.lower()
                # "cliquez sur le bouton ci-dessous pour continuer vos achats" 문구 확인
                if "cliquez sur le bouton ci-dessous pour continuer vos achats" in page_source:
                    logger.info("프랑스 Continue 문구 감지 - 버튼 찾는 중...")
                    
                    continue_selectors = [
                        "//input[@value='Continuer les achats']",
                        "//button[contains(text(), 'Continuer les achats')]",
                        "//span[contains(text(), 'Continuer les achats')]/ancestor::button",
                        "button[type='submit']",
                        ".a-button-primary input[type='submit']"
                    ]
                    
                    for selector in continue_selectors:
                        try:
                            if selector.startswith('//'):
                                continue_button = self.driver.find_element(By.XPATH, selector)
                            else:
                                continue_button = self.driver.find_element(By.CSS_SELECTOR, selector)
                            
                            if continue_button and continue_button.is_displayed():
                                logger.info(f"프랑스 Continue 버튼 발견: {selector}")
                                try:
                                    continue_button.click()
                                    logger.info("Continue 버튼 클릭 성공")
                                except:
                                    self.driver.execute_script("arguments[0].click();", continue_button)
                                    logger.info("JavaScript Continue 버튼 클릭 성공")
                                
                                time.sleep(3)
                                # 원래 URL로 재접속
                                self.driver.get(url)
                                time.sleep(random.uniform(2, 4))
                                break
                        except:
                            continue
            except Exception as e:
                logger.debug(f"Continue 버튼 처리 중 오류: {e}")
            
            page_source_lower = self.driver.page_source.lower()
            continue_patterns = [
                'continue shopping', 'weiter shoppen', 'continuar comprando', 'seguir comprando'
            ]
            
            if any(pattern in page_source_lower for pattern in continue_patterns):
                logger.info("차단 페이지 감지 - Continue 버튼 시도")
                self.handle_captcha_or_block_page()
                time.sleep(3)
            
            self.wait_for_page_load()
            
            if self.is_page_blocked():
                raise Exception("페이지 차단됨")
            
            now_time = datetime.now(self.korea_tz)
            
            result = {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': self.country_code,
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
            
            result['title'] = self.extract_element_text(
                self.selectors[self.country_code].get('title', []), 
                "제목"
            )
            
            has_stock = self.check_stock_availability()
            
            result['retailprice'] = self.extract_price(self.country_code)
            
            if result['retailprice']:
                try:
                    price_value = float(re.sub(r'[^\d.]', '', str(result['retailprice'])))
                    price_ranges = {
                        'gb': (5, 50000), 'de': (10, 10000), 'es': (10, 10000),
                        'usa': (5, 50000), 'jp': (100, 1000000), 'in': (100, 500000)
                    }
                    min_price, max_price = price_ranges.get(self.country_code, (5, 50000))
                    
                    if price_value < min_price or price_value > max_price:
                        logger.warning(f"비정상적인 가격: {result['retailprice']}")
                        result['retailprice'] = None
                except:
                    result['retailprice'] = None
            
            if not has_stock and result['retailprice'] is None:
                result['retailprice'] = "0"
            
            # Ships From 추출 (디버깅 강화)
            logger.info("=== Ships From 추출 시작 ===")
            ships_from_result = self.extract_element_text(
                self.selectors[self.country_code].get('ships_from', []), 
                "Ships From"
            )
            result['ships_from'] = ships_from_result
            logger.info(f"Ships From 최종 결과: '{ships_from_result}'")
            
            # Sold By 추출 (디버깅 강화)
            logger.info("=== Sold By 추출 시작 ===")
            sold_by_result = self.extract_element_text(
                self.selectors[self.country_code].get('sold_by', []), 
                "Sold By"
            )
            result['sold_by'] = sold_by_result
            logger.info(f"Sold By 최종 결과: '{sold_by_result}'")
            
            # 두 번째 요청사항: ships_from과 sold_by 모두 값이 없는 경우 price를 None으로 설정
            if result['ships_from'] is None and result['sold_by'] is None:
                result['retailprice'] =None
                logger.info("ships_from과 sold_by 모두 없어서 가격을 None으로 설정")
            
            for selector in self.selectors[self.country_code].get('imageurl', []):
                try:
                    if selector.startswith('//'):
                        element = self.driver.find_element(By.XPATH, selector)
                    else:
                        element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    result['imageurl'] = element.get_attribute('src')
                    if result['imageurl']:
                        break
                except:
                    continue
            
            page_source = self.driver.page_source.lower()
            # for vat_text in self.selectors[self.country_code].get('vat_text_list', []):
            #     if vat_text.lower() in page_source:
            #         result['vat'] = 'o'
            #         break
            
            logger.info(f"제목: {result['title']}")
            logger.info(f"가격: {result['retailprice']}")
            logger.info(f"Ships From: {result['ships_from']}")
            logger.info(f"Sold By: {result['sold_by']}")
            logger.info("=== 추출 완료 ===")
            
            return result
            
        except Exception as e:
            logger.error(f"페이지 처리 오류: {e}")
            
            if retry_count < max_retries:
                wait_time = (retry_count + 1) * 10
                logger.info(f"{wait_time}초 후 재시도... ({retry_count + 1}/{max_retries})")
                time.sleep(wait_time)
                return self.extract_product_info(url, row_data, retry_count + 1, max_retries)
            
            now_time = datetime.now(self.korea_tz)
            return {
                'retailerid': row_data.get('retailerid', ''),
                'country_code': self.country_code,
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
    
    def get_crawl_targets(self, limit=None):
        """DB에서 크롤링 대상 조회"""
        try:
            query = f"""
            SELECT *
            FROM samsung_price_tracking_list
            WHERE country = '{self.country_code}' 
              AND mall_name = 'amazon'
              AND is_active = TRUE
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, self.db_engine)
            logger.info(f"크롤링 대상 {len(df)}개 조회")
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"크롤링 대상 조회 실패: {e}")
            return []
    
    def save_to_db(self, df):
        """DB에 결과 저장"""
        if self.db_engine is None:
            logger.warning("DB 연결 없음")
            return False
        
        try:
            table_name = f'amazon_price_crawl_tbl_{self.country_code}'
            df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
            logger.info(f"DB 저장 완료: {len(df)}개")
            return True
        except Exception as e:
            logger.error(f"DB 저장 실패: {e}")
            return False
    
    def upload_to_file_server(self, local_file_path, remote_filename=None):
        """파일서버에 업로드"""
        try:
            transport = paramiko.Transport((FILE_SERVER_CONFIG['host'], FILE_SERVER_CONFIG['port']))
            transport.connect(
                username=FILE_SERVER_CONFIG['username'],
                password=FILE_SERVER_CONFIG['password']
            )
            sftp = paramiko.SFTPClient.from_transport(transport)
            
            if remote_filename is None:
                remote_filename = os.path.basename(local_file_path)
            
            country_dir = f"{FILE_SERVER_CONFIG['upload_path']}/{self.country_code}"
            
            try:
                sftp.stat(country_dir)
            except FileNotFoundError:
                logger.info(f"디렉토리 생성: {country_dir}")
                sftp.mkdir(country_dir)
            
            remote_path = f"{country_dir}/{remote_filename}"
            
            sftp.put(local_file_path, remote_path)
            logger.info(f"파일서버 업로드 완료: {remote_path}")
            
            sftp.close()
            transport.close()
            
            return True
        except Exception as e:
            logger.error(f"파일서버 업로드 실패: {e}")
            return False
    
    def save_results(self, df, save_db=True, upload_server=True):
        """결과 저장"""
        now = datetime.now(self.korea_tz)
        base_filename = f"{now.strftime('%Y%m%d%H%M%S')}_{self.country_code}_amazon"
        
        results = {'db_saved': False, 'server_uploaded': False}
        
        if save_db:
            results['db_saved'] = self.save_to_db(df)
        
        if upload_server:
            try:
                csv_filename = f'{base_filename}.csv'
                df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                
                if self.upload_to_file_server(csv_filename):
                    results['server_uploaded'] = True
                
                # excel_filename = f'{base_filename}.xlsx'
                # with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
                #     df.to_excel(writer, sheet_name='All_Results', index=False)
                    
                #     price_df = df[df['retailprice'].notna()]
                #     if not price_df.empty:
                #         price_df.to_excel(writer, sheet_name='With_Prices', index=False)
                
                # self.upload_to_file_server(excel_filename)
                
                if os.path.exists(csv_filename):
                    os.remove(csv_filename)
                # if os.path.exists(excel_filename):
                #     os.remove(excel_filename)
                
                logger.info("임시 파일 삭제 완료")
            except Exception as e:
                logger.error(f"파일 저장 실패: {e}")
        
        return results
    
    def scrape_urls(self, urls_data, max_items=None):
        """URL 스크래핑"""
        if max_items:
            urls_data = urls_data[:max_items]
        
        logger.info(f"크롤링 시작 - {len(urls_data)}개 URL")
        
        if not self.setup_driver():
            logger.error("드라이버 설정 실패")
            return None
        
        results = []
        failed_urls = []
        
        try:
            for idx, row in enumerate(urls_data):
                logger.info(f"진행률: {idx + 1}/{len(urls_data)}")
                
                url = row.get('url')
                result = self.extract_product_info(url, row)
                
                if result['retailprice'] is None and result['title'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'reason': '가격과 제목 모두 없음'
                    })
                elif result['retailprice'] is None:
                    failed_urls.append({
                        'url': url,
                        'item': row.get('item', ''),
                        'reason': '가격 없음'
                    })
                
                results.append(result)
                
                if (idx + 1) % 10 == 0:
                    interim_df = pd.DataFrame(results[-10:])
                    if self.db_engine:
                        try:
                            table_name = f'amazon_price_crawl_tbl_{self.country_code}'
                            interim_df.to_sql(table_name, self.db_engine, if_exists='append', index=False)
                            logger.info("중간 저장: 10개 레코드")
                        except Exception as e:
                            logger.error(f"중간 저장 실패: {e}")
                
                if idx < len(urls_data) - 1:
                    wait_time = random.uniform(5, 10)
                    time.sleep(wait_time)
                    
                    if (idx + 1) % 20 == 0:
                        logger.info("20개 처리 완료, 30초 휴식")
                        time.sleep(30)
        
        except Exception as e:
            logger.error(f"스크래핑 중 오류: {e}")
        
        finally:
            if failed_urls:
                logger.warning(f"실패 URL {len(failed_urls)}개:")
                for fail in failed_urls[:3]:
                    logger.warning(f"  - {fail['item']}: {fail['reason']}")
            
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
        
        return pd.DataFrame(results)
    
    def analyze_results(self, df):
        """결과 분석"""
        logger.info("결과 분석 시작")
        
        total = len(df)
        with_price = df['retailprice'].notna().sum()
        success_rate = (with_price / total * 100) if total > 0 else 0
        
        logger.info(f"전체: {total}개")
        logger.info(f"가격 성공: {with_price}개")
        logger.info(f"성공률: {success_rate:.1f}%")
        
        if with_price > 0:
            try:
                price_df = df[df['retailprice'].notna()].copy()
                price_df['price_numeric'] = price_df['retailprice'].astype(str).str.replace(',', '').astype(float)
                
                logger.info("가격 통계:")
                logger.info(f"  평균: {price_df['price_numeric'].mean():.2f}")
                logger.info(f"  최저: {price_df['price_numeric'].min():.2f}")
                logger.info(f"  최고: {price_df['price_numeric'].max():.2f}")
            except Exception as e:
                logger.warning(f"가격 통계 오류: {e}")

def main():
    """메인 실행 함수"""
    country_code = os.getenv('COUNTRY_CODE', 'de').lower()
    test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'  # 기본값 false로 변경
    max_items = int(os.getenv('MAX_ITEMS', '0')) or None  # 0이면 전체
    
    print("=" * 60)
    print("Amazon 크롤러 v6.0 (전체 크롤링 모드)")
    print("=" * 60)
    print(f"국가: {country_code.upper()}")
    print(f"모드: {'테스트' if test_mode else '전체 크롤링'}")
    if max_items:
        print(f"최대: {max_items}개")
    else:
        print("전체 제품 크롤링")
    print("=" * 60)
    
    scraper = AmazonScraper(country_code)
    
    if test_mode:
        logger.info("테스트 모드 실행")
        
        test_urls = {
            'gb': 'https://www.amazon.co.uk/Crucial-X9-4TB-Portable-External/dp/B0CGW18S6J',
            'de': 'https://www.amazon.de/dp/B087DFFJRD',
            'es': 'https://www.amazon.es/dp/B087DFFJRD?th=1',
            'usa': 'https://www.amazon.com/dp/B087DFFJRD'
        }
        
        test_url = test_urls.get(country_code, test_urls['de'])
        
        test_data = [{
            'url': test_url,
            'brand': 'Samsung',
            'item': 'T7 2TB',
            'retailerid': 'TEST001',
            'retailersku': 'TEST001',
            'channel': 'Online',
            'seg_lv1': 'SSD',
            'seg_lv2': 'Consumer',
            'seg_lv3': 'External',
            'capacity': '2TB',
            'form_factor': 'External'
        }]
        
        results_df = scraper.scrape_urls(test_data)
        if results_df is not None and not results_df.empty:
            scraper.analyze_results(results_df)
            scraper.save_results(results_df, save_db=False, upload_server=True)
        return
    
    logger.info("전체 크롤링 시작")
    if scraper.db_engine is None:
        logger.error("DB 연결 실패")
        return
    
    urls_data = scraper.get_crawl_targets(limit=max_items)
    if not urls_data:
        logger.warning("크롤링 대상 없음")
        return
    
    results_df = scraper.scrape_urls(urls_data, max_items)
    if results_df is None or results_df.empty:
        logger.error("크롤링 결과 없음")
        return
    
    scraper.analyze_results(results_df)
    save_results = scraper.save_results(results_df, save_db=True, upload_server=True)
    
    logger.info("저장 결과:")
    logger.info(f"DB: {'성공' if save_results['db_saved'] else '실패'}")
    logger.info(f"파일: {'성공' if save_results['server_uploaded'] else '실패'}")
    logger.info("크롤링 완료!")

if __name__ == "__main__":
    print("필요 패키지:")
    print("pip install undetected-chromedriver selenium pandas pymysql sqlalchemy paramiko openpyxl")
    print()
    print("환경변수 (선택사항):")
    print("export COUNTRY_CODE=de     # 기본값: de")
    print("export TEST_MODE=false     # 기본값: false (전체 크롤링)")
    print("export MAX_ITEMS=100       # 선택사항 (없으면 전체)")
    print()
    print("전체 크롤링 모드로 실행 중...")
    print()
    
    main()