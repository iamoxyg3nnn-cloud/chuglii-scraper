@echo off
title Chuglii Scraper - SupJav Japanese Pipeline
cd /d "%~dp0..\..\.."

:menu
cls
echo =================================================================
echo       CHUGLII SCRAPER - SUPJAV COMPREHENSIVE PIPELINE
echo       Working Mirrors: VOE HD, VidSpeed, LuluStream Only
echo =================================================================
echo.
echo   [1] QUICK SYNC: Ingest New Releases (Page 1 across all 5 feeds)
echo   [2] 24/7 WATCHER DAEMON: Auto-detect and ingest new incoming videos
echo   [3] DEEP SYNC: Ingest 3-5 pages across all feeds (~350+ videos)
echo   [4] COMPLETE ARCHIVE BACKFILL: Continuous deep scrape with checkpoints
echo   [5] CATEGORY SPECIFIC: Choose a specific category to ingest
echo   [6] DATABASE STATUS: View total videos, mirrors, and checkpoint progress
echo   [7] CUSTOM CLI: Run custom parameters
echo   [8] RESET CHECKPOINT: Reset progress counters to page 1
echo   [9] EXIT
echo.
echo =================================================================
set /p opt="Select an option (1-9): "

if "%opt%"=="1" (
    echo.
    echo [*] Running Quick Sync for newest releases across all categories...
    scraper\.venv\Scripts\python.exe scraper/categories/japanese/pipeline.py --category all --pages 1
    echo.
    pause
    goto menu
) else if "%opt%"=="2" (
    echo.
    echo [*] Starting 24/7 continuous watcher daemon (checks every 15 min)...
    echo [*] Press Ctrl+C at any time to pause or exit.
    scraper\.venv\Scripts\python.exe scraper/categories/japanese/pipeline.py --watch 900
    echo.
    pause
    goto menu
) else if "%opt%"=="3" (
    echo.
    echo [*] Running Deep Sync (3 pages per category)...
    scraper\.venv\Scripts\python.exe scraper/categories/japanese/pipeline.py --category all --pages 3
    echo.
    pause
    goto menu
) else if "%opt%"=="4" (
    echo.
    echo [*] Running Continuous Archive Backfill (10 pages per category per cycle)...
    echo [*] Progress is safely saved to checkpoint.json after every page.
    scraper\.venv\Scripts\python.exe scraper/categories/japanese/pipeline.py --category all --pages 10
    echo.
    pause
    goto menu
) else if "%opt%"=="5" (
    cls
    echo =================================================================
    echo   SELECT CATEGORY TO INGEST:
    echo =================================================================
    echo   [1] Uncensored JAV (uncensored-jav)
    echo   [2] Amateur JAV (amateur)
    echo   [3] Censored JAV (censored-jav)
    echo   [4] Reducing Mosaic JAV (reducing-mosaic)
    echo   [5] Popular Hits (popular)
    echo.
    set /p cat_opt="Select category (1-5): "
    set /p cat_pages="How many pages to ingest? (e.g. 3): "
    
    if "%cat_opt%"=="1" set cat_name=uncensored-jav
    if "%cat_opt%"=="2" set cat_name=amateur
    if "%cat_opt%"=="3" set cat_name=censored-jav
    if "%cat_opt%"=="4" set cat_name=reducing-mosaic
    if "%cat_opt%"=="5" set cat_name=popular
    
    echo.
    echo [*] Ingesting %cat_pages% pages of %cat_name%...
    scraper\.venv\Scripts\python.exe scraper/categories/japanese/pipeline.py --category %cat_name% --pages %cat_pages%
    echo.
    pause
    goto menu
) else if "%opt%"=="6" (
    echo.
    scraper\.venv\Scripts\python.exe scraper/categories/japanese/pipeline.py --status
    echo.
    pause
    goto menu
) else if "%opt%"=="7" (
    echo.
    set /p custom_args="Enter CLI arguments (e.g. --category uncensored-jav --from-page 5 --to-page 10): "
    scraper\.venv\Scripts\python.exe scraper/categories/japanese/pipeline.py %custom_args%
    echo.
    pause
    goto menu
) else if "%opt%"=="8" (
    echo.
    scraper\.venv\Scripts\python.exe scraper/categories/japanese/pipeline.py --reset-checkpoint
    echo.
    pause
    goto menu
) else if "%opt%"=="9" (
    exit /b 0
) else (
    echo Invalid selection.
    pause
    goto menu
)
