@echo off
setlocal
cd /d "%~dp0"

set BBY_GRAPHQL_REGISTRY_DIR=%~dp0mapping_run
set BBY_DISCOVERY_MAX_PRODUCTS=12
set BBY_DISCOVERY_LOAD_SECONDS=10
set BBY_DISCOVERY_ACTION_SECONDS=8

python bby_graphql_full_discovery.py %*
