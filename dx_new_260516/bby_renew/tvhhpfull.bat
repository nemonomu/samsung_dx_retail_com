@echo off
setlocal

cd /d "%~dp0"

git pull
if not exist logs mkdir logs

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Stop';" ^
  "$allLog='logs\bestbuy_tv_hhp_full.log';" ^
  "$cats=@('TV','HHP');" ^
  "Set-Content -Path $allLog -Value ('start=' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'));" ^
  "$total=[Diagnostics.Stopwatch]::StartNew();" ^
  "foreach ($cat in $cats) {" ^
  "  $catLog='logs\bestbuy_' + $cat.ToLower() + '_full.log';" ^
  "  $catStart=Get-Date;" ^
  "  ('===== ' + $cat + ' start ' + $catStart.ToString('yyyy-MM-dd HH:mm:ss') + ' =====') | Tee-Object -FilePath $allLog -Append;" ^
  "  @('BESTBUY_RUN_ROOT','BESTBUY_PROMOTION_RUN_ROOT','BESTBUY_DETAIL_RUN_ROOT','BESTBUY_OUTPUT_ROOT','BESTBUY_SEARCH_SORT','BESTBUY_MAIN_PAGES','BESTBUY_MAIN_RUN_ID','BESTBUY_MAIN_ORGANIC_OFFSET','BESTBUY_FINAL_MAIN_RUN_ID','BESTBUY_FINAL_BSR_RUN_ID','BESTBUY_BSR_RUN_ID') | ForEach-Object { Remove-Item ('Env:' + $_) -ErrorAction SilentlyContinue };" ^
  "  $env:BESTBUY_URL_SOURCE='csv';" ^
  "  $env:BESTBUY_SEARCH_URL='';" ^
  "  if ($cat -eq 'TV') { $env:BESTBUY_SEARCH_TERM='tv'; $steps=@('01','02','03','04','05','06','07','08','09','10','13','14','15') } else { $env:BESTBUY_SEARCH_TERM='cellphone'; $steps=@('01','02','03','04','06','07','08','09','10','13','14') }" ^
  "  $env:BESTBUY_MAIN_SOURCE_HTML='';" ^
  "  $env:BESTBUY_FORCE_REFRESH='1';" ^
  "  $env:BESTBUY_FORCE_STEP_ENV='1';" ^
  "  $env:BESTBUY_FINAL_TARGET_SIZE='300';" ^
  "  $env:BESTBUY_DETAIL_LIMIT='';" ^
  "  $env:BESTBUY_DETAIL_WORKERS='3';" ^
  "  $env:ZENROWS_TIMEOUT='240';" ^
  "  & python -u -m bestbuy.bestbuy_orchestrator --category $cat @steps 2>&1 | Tee-Object -FilePath $catLog | Tee-Object -FilePath $allLog -Append;" ^
  "  if ($LASTEXITCODE -ne 0) { throw ('pipeline failed for ' + $cat + ' exit=' + $LASTEXITCODE) }" ^
  "  $catElapsed=(Get-Date)-$catStart;" ^
  "  ('===== ' + $cat + ' elapsed=' + $catElapsed.ToString() + ' =====') | Tee-Object -FilePath $allLog -Append;" ^
  "}" ^
  "$total.Stop();" ^
  "('end=' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')) | Tee-Object -FilePath $allLog -Append;" ^
  "('elapsed=' + $total.Elapsed.ToString()) | Tee-Object -FilePath $allLog -Append;"

endlocal
