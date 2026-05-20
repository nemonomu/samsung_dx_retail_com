@echo off
setlocal
cd /d "%~dp0"
call bestbuy_run_category.bat TV
exit /b %ERRORLEVEL%
