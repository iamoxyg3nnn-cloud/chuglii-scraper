@echo off
setlocal
title Viral MMS Scraper Runner
color 0b

cd /d "%~dp0..\..\.."

echo ============================================================================
echo   Viral MMS Ingestion Pipeline
echo   Target: https://mmsvibex.life
echo   Target Database: Supabase (scraper.videos) [Parent: 'viral-mms']
echo ============================================================================
echo.

set PYTHON="scraper\.venv\Scripts\python.exe"
if not exist %PYTHON% (
    set PYTHON=python
)

set PAGES=1
set /p PAGES="Enter number of pages to scrape [Default 1, 0 for all]: "

set CATEGORY=all
set /p CATEGORY="Enter category or filter (e.g. all, pakistani, indian, popular) [Default all]: "

echo.
if "%CATEGORY%"=="all" (
    echo [*] Scraping %PAGES% page(s) of global latest feed...
    %PYTHON% "scraper/categories/viral-mms/pipeline.py" --pages %PAGES%
) else if "%CATEGORY%"=="popular" (
    echo [*] Scraping %PAGES% page(s) of popular feed...
    %PYTHON% "scraper/categories/viral-mms/pipeline.py" --pages %PAGES% --filter popular
) else (
    echo [*] Scraping %PAGES% page(s) of category '%CATEGORY%'...
    %PYTHON% "scraper/categories/viral-mms/pipeline.py" --pages %PAGES% --category %CATEGORY%
)

echo.
pause
