# Best Buy GraphQL QA Manual

이 문서는 Best Buy TV 수집을 DOM/XPath 중심에서 GraphQL/API-first로 옮길 때 검수 기준을 정리한다.

## 원칙

- 기존 수집 원천은 DB selector가 기준이다.
- `main`, `bsr` listing selector는 `dx_xpath_selectors` 테이블에서 로드한다.
- `pmt`, `trend`, `detail` selector/config는 `bby_tv_config` 테이블에서 로드한다.
- API-only 테스트에서는 `tv_item_mst` 같은 기존 DB fallback으로 빈 값을 채우지 않는다.
- CSV 값은 GraphQL 응답 또는 listing row에서 실제 관측된 값만 저장한다.
- raw GraphQL response는 검수/파서 개선/스키마 변경 감지용으로 별도 보관한다.

## Selector Sources

### dx_xpath_selectors

사용 코드: `running/common/base_crawler.py::load_xpaths()`

조회 조건:

```sql
SELECT data_field, xpath, previous_xpath
FROM dx_xpath_selectors
WHERE corp = 'SEA'
  AND product_line = 'TV'
  AND account_name = 'Bestbuy'
  AND page_type = ?
  AND is_active = TRUE;
```

대상 page type:

- `main`
- `bsr`
- `promotion`
- `trend`
- `detail`

### bby_tv_config

사용 코드: `bby_config_loader.py`

주요 조건:

```sql
SELECT category, config_key, config_value, file_name, priority
FROM bby_tv_config
WHERE is_active = TRUE
ORDER BY category, config_key, file_name, priority;
```

대상 file name:

- `bby_tv_main1`
- `bby_tv_bsr1`
- `bby_tv_pmt1`
- `bby_tv_trend_crawl`
- `bby_tv_dt1`

## Listing Fields

| CSV field | 기존 source | selector/config key | API-first 기준 |
|---|---|---|---|
| `retailer_sku_name` | listing DOM | `retailer_sku_name`, `product_title`, `product_name` | listing 값을 canonical로 사용 |
| `product_url` | listing DOM | `product_url`, `product_link` | listing URL 사용 |
| `item` | URL tail | code extraction | URL tail 사용 |
| `numeric_sku` | listing DOM/URL/text | `bby_listing_sku.py` | GraphQL key로 사용 |
| `offer` | listing DOM | `offer` | listing 값 사용 |
| `pick_up_availability` | listing DOM | `pick_up_availability`, `pickup_availability` | listing 값 사용 |
| `fastest_delivery` | listing DOM | `fastest_delivery`, legacy `shipping_availability` | listing 값 사용 |
| `delivery_availability` | listing DOM | `delivery_availability` | listing 값 사용 |
| `sku_status` | listing DOM | `sku_status`, `sponsored` | listing 값 사용 |
| `main_rank` | listing order | code | listing 값 사용 |
| `bsr_rank` | listing order | code | listing 값 사용 |
| `promotion_position` | promotion order | code | listing 값 사용 |
| `promotion_type` | promotion DOM | `promotion_type_*`, `section_*` | listing 값 사용 |
| `trend_rank` | trend DOM | `rank` | listing 값 사용 |

Listing 값은 상품 식별의 기준이다. GraphQL의 product name은 검수/정합성 확인용이며, listing name을 무조건 덮어쓰지 않는다.

## Detail Fields

| CSV field | 기존 selector/config key | GraphQL operation 후보 | GraphQL path 후보 | 처리 기준 |
|---|---|---|---|---|
| `retailer_sku_name` | `retailer_sku_name`, `product_title` | `ProductSchema_init`, `getProduct`, `getPDPProductBySkuId` | `productBySkuId.name.short` | listing name 유지, GraphQL name은 mismatch 검수 |
| `sku` | `sku_model_number`, `model_number`, `sku` | `ProductSchema_init`, `ProductSpecification_Init` | `manufacturer.modelNumber`, specification label `Model Number` | 모델번호 저장 |
| `screen_size` | `screen_size`, `screen_size_modal` | `ProductSpecification_Init`, `ProductSchema_init`, `GetCompareProduct` | specification label `Screen Size Class` | GraphQL 응답에서만 저장 |
| `estimated_annual_electricity_use` | `electricity_use`, `estimated_annual_electricity_use` | `ProductSpecification_Init`, `ProductSchema_init`, `GetCompareProduct` | specification label `Estimated Annual Electricity Use` | 숫자만 저장 |
| `model_year` | `model_year` | `ProductSpecification_Init`, `ProductSchema_init` | specification label `Model Year` | 연도 저장 |
| `final_sku_price` | `final_price_inner`, `see_price_in_cart`, `no_longer_available` | `getProduct`, `getPDPProductBySkuId`, price content ops | `price.displayableCustomerPrice`, `price.currentPrice`, restricted price text | 달러 가격 또는 상태값 저장 |
| `original_sku_price` | `original_price_inner` | `getProduct` | `price.displayableRegularPrice`, `price.regularPrice` | 있으면 저장 |
| `savings` | `savings_inner` | `getProduct` | `price.totalSavings` | 있으면 저장 |
| `star_rating` | `star_rating_*`, `top_star_rating` | `CustomerRatingCard_Init`, `ReviewStats_Init`, `ProductSchema_init` | `averageRating`, `ratingValue` | Best Buy 자체 rating만 저장 |
| `count_of_reviews` | `count_of_reviews_*`, `top_count_of_reviews` | `CustomerRatingCard_Init`, `ReviewStats_Init`, `ProductSchema_init` | `reviewCount`, `totalReviewCount` | Best Buy 자체 review count만 저장 |
| `detailed_review_content` | `review_items`, `detailed_review_content*` | `CustomerReviewList_Init` | review text/body/content fields | 최대 20개 |
| `summarized_review_content` | `summarized_review*` | `Ai_Review_Summary_Init` | review summary text | current CSV target excludes this column |
| `recommendation_intent` | `recommendation_intent*` | `CustomerRatingCard_Init` | `recommendedPercent` | `% would recommend to a friend` 형식 |
| `top_mentions` | `top_mentions*` | `Reviews_Pros_Cons_Init` | pros/cons feature fields | current CSV target excludes this column |
| `retailer_sku_name_similar` | `similar_product_names`, `similar_products_container` | `GetCompareProduct`, `ProductCarousel_Recommendations`, `URE_FetchRecommendations` | recommendation product names | 있으면 ` ||| ` join |

## Price State Rules

`final_sku_price`는 숫자 가격만 저장하는 컬럼이 아니다. 기존 DOM/XPath 로직 기준으로 다음 상태값도 정상 수집값이다.

- `See price in cart`
- `See details in checkout`
- `no longer available`

GraphQL/API-only 파서도 raw response 안에서 위 텍스트가 관측되면 그대로 저장한다.

## Review State Rules

다음은 실패가 아니라 정상 상태 처리다.

### Not yet reviewed

저장 기준:

- `star_rating = Not yet reviewed`
- `count_of_reviews = 0`
- `count_of_star_ratings = 0`
- `detailed_review_content = None`
- `recommendation_intent = None`

### External reviews

예: `reviews from Skyworth USA`

기존 로직 기준으로 Best Buy 자체 리뷰가 아니므로 not-yet-reviewed와 같은 형태로 저장한다.

## Mismatch Checks

API-only detail 저장 전 또는 QA audit에서 아래를 검사한다.

| Check | 기준 |
|---|---|
| listing item vs GraphQL skuId | listing의 `numeric_sku`와 GraphQL 요청 `skuId`가 같아야 함 |
| listing name vs GraphQL name | 완전 일치 강제는 하지 않지만, 다른 라인업/모델이면 `product_mismatch` |
| listing item URL vs response URL | GraphQL response의 PDP URL이 있으면 item tail 비교 |
| model number plausibility | `sku`가 Best Buy numeric sku로 남으면 `model_number_missing` |
| price partial | `savings` 또는 `original_sku_price`는 있는데 `final_sku_price`가 비면 `price_partial` |
| review partial | `count_of_reviews > 0`인데 `detailed_review_content`가 비면 `review_partial` |

## Raw Data Layout

권장 저장 위치:

```text
mapping_run/raw_graphql/
```

파일명:

```text
{batch_id}_{item}_{numeric_sku}_{operation}.json
```

예:

```text
20260517_153000_J3ZYG2V5VV_6639210_getProduct.json
```

row 단위 audit:

```text
mapping_run/api_only_audit.jsonl
```

포함해야 할 값:

- `batch_id`
- `item`
- `product_url`
- `listing_name`
- `numeric_sku`
- operation별 status/error
- parsed field map
- missing fields
- mismatch flags

## QA Commands

CSV fill-rate:

```powershell
$p='C:\samsung_dx_retail_com\bby_vpn\v2\mapping_run\bby_tv_vpn_test.csv'
$rows=Import-Csv -LiteralPath $p
'rows=' + $rows.Count
foreach($c in 'star_rating','count_of_reviews','detailed_review_content','recommendation_intent','sku','final_sku_price','original_sku_price','savings','screen_size','estimated_annual_electricity_use','retailer_sku_name_similar'){
  $n=($rows | Where-Object { $_.$c -and $_.$c.Trim() }).Count
  "$c=$n/$($rows.Count)"
}
```

Price partial check:

```powershell
$rows | Where-Object { -not $_.final_sku_price -and ($_.savings -or $_.original_sku_price) } |
  Select-Object order,item,retailer_sku_name,final_sku_price,original_sku_price,savings
```

Review partial check:

```powershell
$rows | Where-Object { [int]($_.count_of_reviews -as [int]) -gt 0 -and -not $_.detailed_review_content } |
  Select-Object order,item,retailer_sku_name,star_rating,count_of_reviews
```

Model number missing check:

```powershell
$rows | Where-Object { $_.sku -match '^\d{6,}$' -or -not $_.sku } |
  Select-Object order,item,sku,retailer_sku_name
```
