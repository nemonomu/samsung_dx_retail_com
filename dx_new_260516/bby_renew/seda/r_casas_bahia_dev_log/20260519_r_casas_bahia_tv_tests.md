# 2026-05-19 R Casas Bahia TV Tests

## 2026-05-19 Asia/Seoul - scaffold setup

- target channel/product/step/run root: `r_casas_bahia` / `TV` / scaffold config and orchestrator / `r_casas_bahia/data/tv/20260519`
- command or code path used: created `r_casas_bahia/step00_config.py`, `r_casas_bahia/r_casas_bahia_orchestrator.py`, `r_casas_bahia/config/r_casas_bahia_initial_urls.csv`
- key environment variables and request conditions: `R_CASAS_BAHIA_PRODUCT_TYPE=TV`, `R_CASAS_BAHIA_RUN_DATE` defaults to local date, no live crawler run
- request variant/browser/API/proxy/header/cookie settings: none; no site request executed from local runtime
- result: success; scaffold files created; status code n/a; row counts n/a; elapsed time n/a; error body n/a
- raw artifacts and manifests created: none
- code files changed: `br_common/*`, `r_casas_bahia/*`, `README.md`
- interpretation and next recommended action: verify Casas Bahia TV listing URL and implement step01 parser after capturing response shape.

## 2026-05-19 Asia/Seoul - Sao Paulo browser access observation

- target channel/product/step/run root: `r_casas_bahia` / `TV` / browser access check / `r_casas_bahia/data/tv/20260519`
- command or code path used: manual Chrome/RDP access to Casas Bahia listing/home path
- key environment variables and request conditions: São Paulo EC2 public IP, normal browser access, no scripted transport
- request variant/browser/API/proxy/header/cookie settings: Chrome browser; no proxy/cookie override recorded
- result: failure; browser rendered error page `Ops! Algo deu errado.`
- error body: page advises closing browser, checking URL, clearing browsing data, or contacting WhatsApp; `Reference ID:0.f90f1002.1779179005.afc49833`; `Client IP:135.136.15.135`
- raw artifacts and manifests created: none captured from browser
- code files changed: none for this browser observation
- interpretation and next recommended action: Casas Bahia also blocks the São Paulo EC2 browser session before product listing data is available. Need alternate egress/session strategy or a manually captured successful browser/API request from an allowed network before parser work.

## 2026-05-19 Asia/Seoul - Sao Paulo urllib listing probe

- target channel/product/step/run root: `r_casas_bahia` / `TV` / `step01_main_list` / `r_casas_bahia/data/tv/20260519`
- command or code path used: `python -m r_casas_bahia.r_casas_bahia_orchestrator --product-type TV --all`
- key environment variables and request conditions: default `R_CASAS_BAHIA_PRODUCT_TYPE=TV`, `R_CASAS_BAHIA_MAIN_PAGES=1`, default Python `urllib` transport, São Paulo EC2/RDP environment
- request variant/browser/API/proxy/header/cookie settings: static desktop Chrome-like headers from `br_common/main_list_probe.py`; no browser session, no proxy, no cookies
- result: failure; status code `403`; row count `0`; elapsed time `1.148s`; response bytes `3146`
- error body: Casas Bahia error page `Ops! Algo deu errado.` with browser-cleanup guidance; same family as manual browser observation
- raw artifacts and manifests created: `r_casas_bahia/data/tv/20260519/main/manifest.json`, `r_casas_bahia/data/tv/20260519/main/raw/main_page_001.html`, `r_casas_bahia/data/tv/20260519/main/parsed/main_occurrences.csv`
- code files changed: none for this remote run
- interpretation and next recommended action: direct HTTP and normal browser access are blocked from current São Paulo EC2 IP. Parser work should wait for an allowed response capture or an alternate egress path.
