@echo off
setlocal
cd /d "%~dp0"
call bestbuy_run_category.bat HHP
exit /b %ERRORLEVEL%
