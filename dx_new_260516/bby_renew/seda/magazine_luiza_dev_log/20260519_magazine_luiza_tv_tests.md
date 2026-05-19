# 2026-05-19 Magazine Luiza TV Tests

## 2026-05-19 Asia/Seoul - scaffold setup

- target channel/product/step/run root: `magazine_luiza` / `TV` / scaffold config and orchestrator / `magazine_luiza/data/tv/20260519`
- command or code path used: created `magazine_luiza/step00_config.py`, `magazine_luiza/magazine_luiza_orchestrator.py`, `magazine_luiza/config/magazine_luiza_initial_urls.csv`
- key environment variables and request conditions: `MAGAZINE_LUIZA_PRODUCT_TYPE=TV`, `MAGAZINE_LUIZA_RUN_DATE` defaults to local date, no live crawler run
- request variant/browser/API/proxy/header/cookie settings: none; no site request executed from local runtime
- result: success; scaffold files created; status code n/a; row counts n/a; elapsed time n/a; error body n/a
- raw artifacts and manifests created: none
- code files changed: `br_common/*`, `magazine_luiza/*`, `README.md`
- interpretation and next recommended action: implement and test step01 listing collection against Magazine Luiza TV search.
