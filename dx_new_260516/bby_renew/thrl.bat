@echo off
setlocal EnableExtensions

cd /d "%~dp0"

if not exist logs mkdir logs
if not "%BESTBUY_SKIP_PULL%"=="1" git pull

set BESTBUY_SKIP_PULL=1

call bestbuy_run_category.bat TV
if errorlevel 1 exit /b %ERRORLEVEL%

call bestbuy_run_category.bat HHP
if errorlevel 1 exit /b %ERRORLEVEL%

call bestbuy_run_category.bat REF
if errorlevel 1 exit /b %ERRORLEVEL%

call bestbuy_run_category.bat LDY
if errorlevel 1 exit /b %ERRORLEVEL%

exit /b 0
