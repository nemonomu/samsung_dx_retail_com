@echo off
setlocal

cd /d "%~dp0"

git pull
if not exist logs mkdir logs

set BESTBUY_URL_SOURCE=csv
set BESTBUY_FORCE_REFRESH=1
set BESTBUY_FINAL_TARGET_SIZE=300
set BESTBUY_DETAIL_LIMIT=
set BESTBUY_DETAIL_WORKERS=3
set ZENROWS_TIMEOUT=240

python -u -m bestbuy.bestbuy_orchestrator --category TV 01 02 03 04 05 06 07 08 09 10 13 14 2>&1 | powershell -NoProfile -Command "$input | Tee-Object -FilePath 'logs\bestbuy_full_collect_db.log'"

endlocal
