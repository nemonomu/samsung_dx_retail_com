"""
bby_tv_dt1.py 누락 XPath 설정 추가 스크립트
"""
import psycopg2
from config import DB_CONFIG

def add_xpath_configs():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cursor = conn.cursor()

    file_name = 'bby_tv_dt1'

    # 누락된 XPath 설정들
    xpath_configs = [
        # === extract_retailer_sku_name() ===
        ('retailer_sku_name', '//h1[contains(@class, "h4")]', 1),
        ('retailer_sku_name', '//div[@class="sku-title"]//h1', 2),

        # === extract_electricity_use() ===
        ('electricity_use', '//div[contains(@class, "dB7j8sHUbncyf79K")]//div[contains(text(), "Estimated Annual Electricity Use")]/following-sibling::div[@class="grow basis-none pl-300"]', 1),
        ('electricity_use', '//li[.//h4[text()="Power"]]//div[.//div[contains(text(), "Estimated Annual Electricity Use")]]//div[@class="grow basis-none pl-300"]', 2),
        ('electricity_use', '//div[contains(text(), "Estimated Annual Electricity Use")]/following-sibling::div[@class="grow basis-none pl-300"]', 3),
        ('electricity_use', '//div[contains(text(), "Estimated Annual Electricity Use")]/..//div[@class="grow basis-none pl-300"]', 4),
        ('electricity_use', '//div[contains(., "Estimated Annual Electricity Use")]//div[contains(@class, "pl-300")]', 5),

        # === extract_screen_size() ===
        ('screen_size', '/html/body/div[5]/div[4]/div[2]/div/div[3]/div[1]/button[2]/div/div/div[2]', 1),
        ('screen_size', '//div[contains(text(), "Screen Size Class")]/following-sibling::div[@class="flex font-500 items-center"]', 2),
        ('screen_size', '//div[text()="Screen Size Class"]/..//div[contains(@class, "flex font-500")]', 3),

        # === extract_model_year() ===
        ('model_year', '//div[contains(text(), "Model Year")]/following-sibling::div', 1),
        ('model_year', '//div[text()="Model Year"]/../div[contains(@class, "grow basis-none")]', 2),
        ('model_year', '//div[contains(@class, "font-weight-medium") and contains(text(), "Model Year")]/../div[contains(@class, "grow basis-none pl-300")]', 3),

        # === extract_sku() ===
        ('sku_model_number', '//div[contains(@class, "dB7j8sHUbncyf79K")][.//div[contains(text(), "Model Number")]]/div[contains(@class, "pl-300")]', 1),
        ('sku_model_number', '//div[contains(text(), "Model Number")]/following-sibling::div[contains(@class, "pl-300")]', 2),
        ('sku_model_number', '//div[contains(text(), "Model Number")]/../div[contains(@class, "pl-300")]', 3),
        ('sku_model_number', '//div[contains(text(), "Model Number")]/following-sibling::div', 4),

        # === extract_final_sku_price() - no_longer_available ===
        ('no_longer_available', '/html/body/div[5]/div[3]/div[1]/div/div[2]/div[4]', 1),
        ('no_longer_available', '//div[@class="text-danger text-4 font-500 leading-4"]', 2),
        ('no_longer_available', '//div[contains(@class, "text-danger")][contains(text(), "no longer available")]', 3),
        ('no_longer_available', '//div[contains(text(), "This item is no longer available in new condition")]', 4),

        # === price_container (공통 컨테이너) ===
        ('price_container', '/html/body/div[5]/div[4]/div[1]', 1),
        ('price_container', '//div[@class="order-2 t3V0AOwowrTfUzPn "]', 2),
        ('price_container', '//div[contains(@class, "order-2")]', 3),

        # === extract_final_sku_price() - price_xpaths ===
        ('final_price_inner', './/div[@data-testid="price-block-customer-price"]//span', 1),
        ('final_price_inner', './/div[@data-lu-target="customer_price"]//span', 2),
        ('final_price_inner', './/span[@class="font-sans text-default text-style-body-md-400 font-500 text-7 leading-7"]', 3),

        # === extract_final_sku_price() - see_price_xpaths ===
        ('see_price_in_cart', './/div[@data-testid="price-restricted-price-tap-for-price"]//span', 1),
        ('see_price_in_cart', './/span[contains(text(), "See price in cart")]', 2),
        ('see_price_in_cart', './/span[contains(text(), "See details in checkout")]', 3),
        ('see_price_in_cart', './/div[@data-testid="price-block"]//span[contains(text(), "See price in cart")]', 4),
        ('see_price_in_cart', './/div[@data-testid="price-block"]//span[contains(text(), "See details in checkout")]', 5),

        # === extract_original_sku_price() - price_xpaths ===
        ('original_price_inner', './/span[@data-lu-target="comp_value"]', 1),
        ('original_price_inner', './/span[@data-testid="price-block-regular-price-message-text"]//span[@data-lu-target="comp_value"]', 2),
        ('original_price_inner', './/span[contains(@style, "color: rgb(108, 111, 117)") and contains(., "$")]', 3),

        # === extract_original_sku_price() - buy_new_xpaths ===
        ('buy_new_price', '/html/body/div[4]/div[4]/div[1]/div/div[4]/div/div/div/div/div[1]/div[2]/div/div/div/div/div[2]/div[2]/div/a', 1),
        ('buy_new_price', '//a[@data-testid="price-block-regular-price-message-link"]//span', 2),
        ('buy_new_price', '//div[@data-testid="price-block-regular-price-link-text-wrapper"]//a//span', 3),

        # === extract_savings() - savings_xpaths ===
        ('savings_inner', './/span[@data-testid="price-block-total-savings-text"]', 1),
        ('savings_inner', './/div[@data-testid="price-block-total-savings"]//span', 2),
        ('savings_inner', './/span[contains(@style, "color: rgb(232, 30, 37)") and contains(., "Save")]', 3),

        # === extract_star_rating() - hidden_xpaths ===
        ('star_rating_hidden', '//p[@class="visually-hidden"][contains(text(), "Rating")]', 1),
        ('star_rating_hidden', '//p[contains(@class, "visually-hidden")][contains(text(), "out of 5 stars")]', 2),

        # === extract_star_rating() - rating_xpaths ===
        ('star_rating_inner', './/div/div[4]/a/div/span[1]', 1),
        ('star_rating_inner', './/div/div[3]/a/div/span[1]', 2),
        ('star_rating_inner', './/span[@class="font-weight-medium  font-weight-bold order-1"]', 3),
        ('star_rating_inner', './/span[contains(@class, "font-weight-bold") and contains(@class, "order-1")]', 4),
        ('star_rating_inner', './/span[@aria-hidden="true"][contains(@class, "order-1")]', 5),
        ('star_rating_inner', './/span[contains(@class, "X1oPXJyKAwAqyfx_")]', 6),
        ('star_rating_inner', './/span[contains(@class, "heading-2") and contains(@class, "font-weight-medium")]', 7),

        # === extract_count_of_reviews_from_detail() - external_review_xpaths ===
        ('external_reviews', './/div/div[3]/a/div/span', 1),
        ('external_reviews', './/span[contains(@class, "c-ratings-reviews")]', 2),
        ('external_reviews', './/span[contains(@class, "c-reviews")]', 3),

        # === extract_count_of_reviews_from_detail() - reviews_xpaths ===
        ('reviews_count_inner', './/div/div[3]/a/div/span[2]', 1),
        ('reviews_count_inner', './/span[@class="c-reviews order-2"]', 2),
        ('reviews_count_inner', './/span[contains(@class, "c-reviews")]', 3),
        ('reviews_count_inner', './/span[@aria-hidden="true"][contains(@class, "order-2")]', 4),
        ('reviews_count_inner', './/div/div[3]/a/div/span', 5),
        ('reviews_count_inner', './/span[contains(@class, "c-ratings-reviews")]', 6),

        # === extract_count_of_reviews_from_detail() - not_reviewed_xpaths ===
        ('not_yet_reviewed', './/div/div[3]/a/div/span', 1),
        ('not_yet_reviewed', './/span[contains(text(), "Not yet reviewed")]', 2),
        ('not_yet_reviewed', './/span[@class="c-reviews order-2"][contains(text(), "Not yet reviewed")]', 3),

        # === close_specifications_dialog() ===
        ('dialog_close_btn', 'xpath://button[@data-testid="brix-sheet-closeButton"]', 1),
        ('dialog_close_btn', 'xpath://button[@aria-label="Close Sheet"]', 2),
        ('dialog_close_btn', 'xpath://div[@class="relative"]//button', 3),

        # === extract_star_rating_from_reviews_page() ===
        ('star_rating_reviews_page', 'xpath://div[@class="overall-rating"]', 1),
        ('star_rating_reviews_page', 'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[1]/div/div[1]', 2),
        ('star_rating_reviews_page', 'xpath://div[contains(@class, "overall-rating")]', 3),

        # === extract_star_ratings_from_reviews_page() - 별점별 개수 ===
        ('star_ratings_5', 'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[1]/div/label/span[5]', 1),
        ('star_ratings_4', 'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[2]/div/label/span[5]', 1),
        ('star_ratings_3', 'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[3]/div/label/span[5]', 1),
        ('star_ratings_2', 'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[4]/div/label/span[5]', 1),
        ('star_ratings_1', 'xpath://*[@id="reviews-accordion"]/section/div[1]/div[1]/div/div/div[2]/div/fieldset/div[5]/div/label/span[5]', 1),

        # === extract_count_of_reviews() - reviews page ===
        ('count_reviews_page', 'xpath://span[@class="c-reviews order-2"]', 1),
        ('count_reviews_page', 'xpath://div[contains(@id, "user-generated-content-ugc-stats")]//span[@class="c-reviews order-2"]', 2),
        ('count_reviews_page', 'xpath://span[contains(@class, "c-reviews")]', 3),

        # === extract_bestbuy_sku_number() ===
        ('bestbuy_sku', 'xpath://div[contains(text(), "SKU:")]', 1),
        ('bestbuy_sku', 'xpath://div[@class="pr-150 inline-block"][contains(text(), "SKU")]', 2),
        ('bestbuy_sku_testid', 'xpath://div[contains(@data-testid, "mbo-entrypoint-")]', 1),

        # === click_see_all_reviews() - 이미 있지만 추가 selector ===
        ('see_all_reviews_btn', 'xpath://button[contains(@class, "Op9coqeII1kYHR9Q")]', 2),
        ('see_all_reviews_btn', 'css:button.Op9coqeII1kYHR9Q', 3),

        # === extract_reviews() ===
        ('review_items', '//li[@class="review-item"]//div[@class="ugc-review-body"]//p[@class="pre-white-space"]', 1),

        # === extract_summarized_review_content() ===
        ('summarized_review', '//div[contains(@class, "review-summary")]//p[contains(@class, "body-copy-lg")]', 1),
        ('summarized_review', '//h4[contains(text(), "Customers are saying")]/following-sibling::p', 2),
        ('summarized_review', '//p[@class="mt-200 body-copy-lg mb-none"]', 3),
        ('summarized_review', '//p[contains(@class, "body-copy-lg") and contains(@class, "mb-none")]', 4),
        ('summarized_review', '/html/body/div[5]/div[8]/div[2]/div/div[1]/div/p', 5),

        # === extract_summarized_review_content_from_reviews_page() ===
        ('summarized_review_reviews_page', '//*[@id="reviews-accordion"]/div[1]/div/p[1]', 1),
        ('summarized_review_reviews_page', '//p[@class="mb-200 mt-none"]', 2),
        ('summarized_review_reviews_page', '//div[@id="reviews-accordion"]//p[contains(@class, "mb-200")]', 3),

        # === extract_recommendation_intent_from_reviews_page() ===
        ('recommendation_intent', '//div[contains(@class, "recommendation-card-no-donut")]//span[@class="recommendation-percent v-fw-medium"]', 1),
        ('recommendation_intent', '//span[contains(@class, "recommendation-percent")]', 2),

        # === extract_similar_products() ===
        ('similar_product_names', '//span[@class="clamp" and starts-with(@id, "compare-title-")]', 1),
        ('similar_product_pros', '//tr[@class="flex"]//td[.//svg[@aria-label="Advantage Icon"]]//span[@class="text-3 min-w-0 flex flex-wrap"]', 1),
        ('similar_product_cons', '//tr[@class="flex"]//td[.//svg[@aria-label="Disadvantage Icon"]]//span[@class="text-3 min-w-0 flex flex-wrap"]', 1),

        # === extract_compare_similar_products() ===
        ('compare_similar_container', 'xpath://div[@class="product-title font-weight-normal pb-100 body-copy-lg min-h-600"]', 1),
    ]

    added_count = 0
    skipped_count = 0

    for config_key, config_value, priority in xpath_configs:
        # 중복 체크
        cursor.execute('''
            SELECT id FROM bby_tv_config
            WHERE category = 'xpath'
              AND config_key = %s
              AND config_value = %s
              AND file_name = %s
        ''', (config_key, config_value, file_name))

        if cursor.fetchone():
            skipped_count += 1
            continue

        # 삽입
        cursor.execute('''
            INSERT INTO bby_tv_config (category, config_key, config_value, file_name, priority, is_active, description)
            VALUES ('xpath', %s, %s, %s, %s, TRUE, %s)
        ''', (config_key, config_value, file_name, priority, f'Auto-added for {config_key}'))
        added_count += 1

    print(f'[OK] Added {added_count} xpath configs, skipped {skipped_count} duplicates')

    cursor.close()
    conn.close()

if __name__ == '__main__':
    add_xpath_configs()
