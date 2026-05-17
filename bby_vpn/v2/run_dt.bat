@echo off
setlocal
cd /d "%~dp0"

set BBY_DT_CLEAR_OUTPUT=1
set BBY_API_ONLY_ALLOW_PAGE_ACCESS=0
set BBY_BROWSER_FETCH_HEADLESS=0

python -c "import step05_listing_detail_flow as f; rows=f.filtered_listing_rows(); print('dt_rows', len(rows)); f.run_api_only_detail(rows)"
set EXIT_CODE=%ERRORLEVEL%

endlocal & exit /b %EXIT_CODE%
