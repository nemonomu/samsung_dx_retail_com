# 2026-05-19 R Magalu HHP Tests

## 2026-05-19 Asia/Seoul - scaffold setup

- target channel/product/step/run root: `r_magalu` / `HHP` / scaffold config and orchestrator / `r_magalu/data/hhp/20260519`
- command or code path used: created `r_magalu/step00_config.py`, `r_magalu/r_magalu_orchestrator.py`, `r_magalu/config/r_magalu_initial_urls.csv`
- key environment variables and request conditions: `R_MAGALU_PRODUCT_TYPE=HHP`, `R_MAGALU_RUN_DATE` defaults to local date, no live crawler run
- request variant/browser/API/proxy/header/cookie settings: none; no site request executed from local runtime
- result: success; scaffold files created; status code n/a; row counts n/a; elapsed time n/a; error body n/a
- raw artifacts and manifests created: none
- code files changed: `br_common/*`, `r_magalu/*`, `README.md`
- interpretation and next recommended action: implement and test step01 listing collection against Magalu HHP search, then record request mode and parser results.

## 2026-05-19 Asia/Seoul - Sao Paulo urllib listing probe

- target channel/product/step/run root: `r_magalu` / `HHP` / `step01_main_list` / `r_magalu/data/hhp/20260519`
- command or code path used: `python -m r_magalu.r_magalu_orchestrator --all`
- key environment variables and request conditions: default `R_MAGALU_PRODUCT_TYPE=HHP`, `R_MAGALU_MAIN_PAGES=1`, default Python `urllib` transport, São Paulo EC2/RDP environment
- request variant/browser/API/proxy/header/cookie settings: static desktop Chrome-like headers from `br_common/main_list_probe.py`; no browser session, no proxy, no cookies
- result: failure; status code `403`; row count `0`; elapsed time `0.119s`; response bytes `1083`
- error body: Magalu Akamai bot page, title `Magazine Luiza | Não é possível acessar a página`, message `Erro 403`, includes `akamai-bot/css/styles-v0.css`
- raw artifacts and manifests created: `r_magalu/data/hhp/20260519/main/manifest.json`, `r_magalu/data/hhp/20260519/main/raw/main_page_001.html`, `r_magalu/data/hhp/20260519/main/parsed/main_occurrences.csv`
- code files changed: none for this remote run
- interpretation and next recommended action: network reaches Magalu but direct HTTP is blocked by Akamai. Next test should use a real browser session/cookie-carrying transport or a captured browser request rather than parser work.

## 2026-05-19 Asia/Seoul - Sao Paulo browser 403 observation

- target channel/product/step/run root: `r_magalu` / `HHP` / browser access check / `r_magalu/data/hhp/20260519`
- command or code path used: manual Chrome/RDP access to `https://www.magazineluiza.com.br/busca/smartphone/`
- key environment variables and request conditions: São Paulo EC2 public IP, normal browser page load, no scripted transport
- request variant/browser/API/proxy/header/cookie settings: Chrome browser; no proxy/cookie override recorded
- result: failure; page rendered Magalu error page `Não é possível acessar a página`, `(Erro 403)`, error ID `0.640e2d17.1779177971.1bf78996`
- observed network calls: after clicking `Falar com o Magalu`, browser showed `graphql?operationName=adsItemsQuery` and `graphql?operationName=trendsQuery` with `200`; these appear related to header/support/error-page widgets, not confirmed product listing data
- raw artifacts and manifests created: none captured from browser
- code files changed: none for this browser observation
- interpretation and next recommended action: Magalu blocks even real browser access from this EC2/IP. Product listing API shape cannot be confirmed from this blocked page. Test Casas Bahia next; for Magalu/Magazine Luiza use a different egress IP/session or capture product listing network traffic from an environment where the page renders normally.

## 2026-05-19 Asia/Seoul - Sao Paulo Magalu homepage browser observation

- target channel/product/step/run root: `r_magalu` / `HHP` / browser access check / `r_magalu/data/hhp/20260519`
- command or code path used: manual Chrome/RDP access to `https://www.magazineluiza.com.br/`
- key environment variables and request conditions: São Paulo EC2 public IP, normal browser page load, no scripted transport
- request variant/browser/API/proxy/header/cookie settings: Chrome browser; homepage navigation first, no proxy/cookie override recorded
- result: success for homepage render; listing/search path still previously returned `403`
- observed network calls: `graphql?operationName=adsEventsMutation` with `200`, initiated from `datadog-rum.js`; small responses around `0.1 kB`, likely analytics/ad event tracking rather than product listing data
- raw artifacts and manifests created: none captured from browser
- code files changed: none for this browser observation
- interpretation and next recommended action: Magalu does not block all browser traffic from the São Paulo IP. Next test should enter `smartphone` through the homepage search UI and capture the resulting search/listing requests, cookies, and final URL. If UI search renders products, reuse those session cookies or discovered API endpoint in `step01_main_list`.
