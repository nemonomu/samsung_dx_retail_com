@echo off
setlocal

cd /d "%~dp0"

git pull
if not exist logs mkdir logs

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference='Stop';" ^
  "$allLog='logs\bestbuy_hhp_ref_ldy_test.log';" ^
  "$cats=@('HHP','REF','LDY');" ^
  "Set-Content -Path $allLog -Value ('start=' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'));" ^
  "$total=[Diagnostics.Stopwatch]::StartNew();" ^
  "foreach ($cat in $cats) {" ^
  "  $catLog='logs\bestbuy_' + $cat.ToLower() + '_test.log';" ^
  "  $catStart=Get-Date;" ^
  "  ('===== ' + $cat + ' start ' + $catStart.ToString('yyyy-MM-dd HH:mm:ss') + ' =====') | Tee-Object -FilePath $allLog -Append;" ^
  "  @('BESTBUY_RUN_ROOT','BESTBUY_PROMOTION_RUN_ROOT','BESTBUY_DETAIL_RUN_ROOT','BESTBUY_OUTPUT_ROOT','BESTBUY_SEARCH_URL','BESTBUY_SEARCH_TERM','BESTBUY_SEARCH_SORT','BESTBUY_MAIN_SOURCE_HTML') | ForEach-Object { Remove-Item ('Env:' + $_) -ErrorAction SilentlyContinue };" ^
  "  $env:BESTBUY_URL_SOURCE='csv';" ^
  "  $env:BESTBUY_FORCE_REFRESH='1';" ^
  "  $env:BESTBUY_FINAL_TARGET_SIZE='50';" ^
  "  $env:BESTBUY_DETAIL_LIMIT='3';" ^
  "  $env:BESTBUY_DETAIL_WORKERS='1';" ^
  "  $env:ZENROWS_TIMEOUT='240';" ^
  "  if ($cat -eq 'HHP') { $steps='01 02 03 04 05 06 07 08 09 10 13 14' } else { $steps='01 02 03 04 07 08 09 10 13 14' }" ^
  "  python -u -m bestbuy.bestbuy_orchestrator --category $cat $steps 2>&1 | Tee-Object -FilePath $catLog | Tee-Object -FilePath $allLog -Append;" ^
  "  if ($LASTEXITCODE -ne 0) { throw ('pipeline failed for ' + $cat + ' exit=' + $LASTEXITCODE) }" ^
  "  $catElapsed=(Get-Date)-$catStart;" ^
  "  ('===== ' + $cat + ' elapsed=' + $catElapsed.ToString() + ' =====') | Tee-Object -FilePath $allLog -Append;" ^
  "}" ^
  "$total.Stop();" ^
  "('end=' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss')) | Tee-Object -FilePath $allLog -Append;" ^
  "('elapsed=' + $total.Elapsed.ToString()) | Tee-Object -FilePath $allLog -Append;"

endlocal
