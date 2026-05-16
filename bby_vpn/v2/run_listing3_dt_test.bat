@echo off
setlocal
cd /d "%~dp0"

set BBY_LISTING_TEST_PAGES=3
python bby_tv_listing3_dt_test.py %*
