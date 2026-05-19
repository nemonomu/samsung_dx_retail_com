# 2026-05-19 Magazine Luiza LDY Tests

## 2026-05-19 Asia/Seoul - scaffold setup

- target channel/product/step/run root: `magazine_luiza` / `LDY` / scaffold config and orchestrator / `magazine_luiza/data/ldy/20260519`
- command or code path used: added LDY seed row to `magazine_luiza/config/magazine_luiza_initial_urls.csv`
- key environment variables and request conditions: `MAGAZINE_LUIZA_PRODUCT_TYPE=LDY`, `MAGAZINE_LUIZA_RUN_DATE` defaults to local date, no live crawler run
- request variant/browser/API/proxy/header/cookie settings: none; no site request executed from local runtime
- result: success; seed row created; status code n/a; row counts n/a; elapsed time n/a; error body n/a
- raw artifacts and manifests created: none
- code files changed: `magazine_luiza/config/magazine_luiza_initial_urls.csv`
- interpretation and next recommended action: implement and test step01 listing collection against Magazine Luiza washer search.
