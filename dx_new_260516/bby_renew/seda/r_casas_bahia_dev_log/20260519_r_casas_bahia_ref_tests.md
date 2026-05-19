# 2026-05-19 R Casas Bahia REF Tests

## 2026-05-19 Asia/Seoul - scaffold setup

- target channel/product/step/run root: `r_casas_bahia` / `REF` / scaffold config and orchestrator / `r_casas_bahia/data/ref/20260519`
- command or code path used: added REF seed row to `r_casas_bahia/config/r_casas_bahia_initial_urls.csv`
- key environment variables and request conditions: `R_CASAS_BAHIA_PRODUCT_TYPE=REF`, `R_CASAS_BAHIA_RUN_DATE` defaults to local date, no live crawler run
- request variant/browser/API/proxy/header/cookie settings: none; no site request executed from local runtime
- result: success; seed row created; status code n/a; row counts n/a; elapsed time n/a; error body n/a
- raw artifacts and manifests created: none
- code files changed: `r_casas_bahia/config/r_casas_bahia_initial_urls.csv`
- interpretation and next recommended action: verify Casas Bahia REF listing URL and parser fields for refrigerator specs.
