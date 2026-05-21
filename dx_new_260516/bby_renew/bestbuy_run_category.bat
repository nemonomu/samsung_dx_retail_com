@echo off
setlocal EnableExtensions

cd /d "%~dp0"

set CAT=%~1
if "%CAT%"=="" (
  echo Usage: bestbuy_run_category.bat TV^|HHP^|REF^|LDY
  exit /b 2
)
shift /1
set EXTRA_ARGS=
:collect_extra_args
if "%~1"=="" goto extra_args_done
set EXTRA_ARGS=%EXTRA_ARGS% %~1
shift /1
goto collect_extra_args
:extra_args_done

if not exist logs mkdir logs
if not "%BESTBUY_SKIP_PULL%"=="1" git pull

set BESTBUY_RUN_ROOT=
set BESTBUY_RUN_DATE=
set BESTBUY_PROMOTION_RUN_ROOT=
set BESTBUY_DETAIL_RUN_ROOT=
set BESTBUY_OUTPUT_ROOT=
set BESTBUY_BATCH_ID=
set BESTBUY_BATCH_TIME=
set BESTBUY_DETAIL_SKUS=
set BESTBUY_DETAIL_FORCE_REFRESH=
set BESTBUY_DETAIL_REBUILD_ONLY=
set BESTBUY_DETAIL_RETRY_ONLY=
set BESTBUY_MAIN_PAGES=
set BESTBUY_MAIN_RUN_ID=
set BESTBUY_MAIN_ORGANIC_OFFSET=
set BESTBUY_SEARCH_SORT=
set BESTBUY_FINAL_MAIN_RUN_ID=
set BESTBUY_FINAL_BSR_RUN_ID=
set BESTBUY_BSR_RUN_ID=
set BESTBUY_ITEM_MST_TABLE=
set BESTBUY_ITEM_MST_TABLE_TV=
set BESTBUY_ITEM_MST_TABLE_HHP=
set BESTBUY_OUTPUT_TABLE=
set BESTBUY_OUTPUT_TABLE_TV=
set BESTBUY_OUTPUT_TABLE_HHP=
set BESTBUY_OUTPUT_TABLE_REF=
set BESTBUY_OUTPUT_TABLE_LDY=
set BESTBUY_PRODUCT_LIST_TABLE=
set BESTBUY_PRODUCT_LIST_TABLE_TV=
set BESTBUY_PRODUCT_LIST_TABLE_HHP=
set BESTBUY_PRODUCT_LIST_TABLE_REF=
set BESTBUY_PRODUCT_LIST_TABLE_LDY=

set BESTBUY_URL_SOURCE=csv
set BESTBUY_SEARCH_URL=
set BESTBUY_MAIN_SOURCE_HTML=
set BESTBUY_FORCE_REFRESH=1
set BESTBUY_FORCE_STEP_ENV=1
set BESTBUY_FINAL_TARGET_SIZE=0
set BESTBUY_DETAIL_LIMIT=
set BESTBUY_DETAIL_WORKERS=3
set BESTBUY_POSTAL_CODE=10010
set BESTBUY_ZIP_CODE=10010
set BESTBUY_CRAWL_DATETIME=
set ZENROWS_TIMEOUT=240

if /I "%CAT%"=="TV" (
  set BESTBUY_SEARCH_TERM=tv
  set BESTBUY_OUTPUT_TABLE_TV=tv_retail_com
  set BESTBUY_PRODUCT_LIST_TABLE_TV=bby_tv_product_list
  set STEPS=01 02 03 04 05 06 07 08 09 10 13 14 15
  set LOG=logs\bestbuy_tv_collect_db.log
) else if /I "%CAT%"=="HHP" (
  set BESTBUY_SEARCH_TERM=cellphone
  set BESTBUY_OUTPUT_TABLE_HHP=hhp_retail_com_bby_v2_test
  set BESTBUY_PRODUCT_LIST_TABLE_HHP=bby_hhp_product_list_v2_test
  set STEPS=01 02 03 04 06 07 08 09 10 13 14 15
  set LOG=logs\bestbuy_hhp_collect_db.log
) else if /I "%CAT%"=="REF" (
  set BESTBUY_SEARCH_TERM=refrigerator
  set STEPS=01 02 03 04 07 08 09 10 13 14
  set LOG=logs\bestbuy_ref_collect_db.log
) else if /I "%CAT%"=="LDY" (
  set BESTBUY_SEARCH_TERM=washing machine
  set STEPS=01 02 03 04 07 08 09 10 13 14
  set LOG=logs\bestbuy_ldy_collect_db.log
) else (
  echo Unknown category: %CAT%
  exit /b 2
)

for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set BESTBUY_RUN_DATE=%%i
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format HHmmss"') do set BESTBUY_BATCH_TIME=%%i
set BESTBUY_BATCH_ID=b_%BESTBUY_RUN_DATE%_%BESTBUY_BATCH_TIME%

echo ===== %CAT% start %date% %time% =====
echo batch_id=%BESTBUY_BATCH_ID%
echo run_date=%BESTBUY_RUN_DATE%
echo batch_time=%BESTBUY_BATCH_TIME%
echo postal_code=%BESTBUY_POSTAL_CODE%
echo log=%LOG%
echo steps=%STEPS%
if not "%EXTRA_ARGS%"=="" echo extra_args=%EXTRA_ARGS%

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Continue';" ^
  "$cmd='python -u -m bestbuy.bestbuy_orchestrator --category %CAT% %STEPS% %EXTRA_ARGS%';" ^
  "Write-Output ('[cmd] ' + $cmd);" ^
  "cmd /c $cmd 2>&1 | Tee-Object -FilePath '%LOG%';" ^
  "exit $LASTEXITCODE"

set EXITCODE=%ERRORLEVEL%
echo ===== %CAT% end %date% %time% exit=%EXITCODE% =====
exit /b %EXITCODE%
