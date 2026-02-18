@echo off
REM Quick ingestion script for production database
REM Usage: ingest_to_production.bat "path\to\file.csv" "Source_Name" [db_number] [template_path]
REM   db_number: Optional - 1 for current db, 2 for new db (if omitted, will prompt)
REM   template_path: Optional - path to mapping template CSV

setlocal

if "%~1"=="" (
    echo Usage: ingest_to_production.bat "path\to\file.csv" "Source_Name" [db_number] [template_path]
    echo.
    echo Examples:
    echo   ingest_to_production.bat "..\leads.csv" "BrokerName"        (will prompt for db, fuzzy mode)
    echo   ingest_to_production.bat "..\leads.csv" "BrokerName" 1      (use current db, fuzzy mode)
    echo   ingest_to_production.bat "..\leads.csv" "BrokerName" 2      (use new db, fuzzy mode)
    echo   ingest_to_production.bat "..\leads.csv" "BrokerName" 1 "templates\broker.csv"  (with template)
    exit /b 1
)

if "%~2"=="" (
    echo Error: Source name is required
    echo Usage: ingest_to_production.bat "path\to\file.csv" "Source_Name" [db_number] [template_path]
    exit /b 1
)

set INPUT_FILE=%~1
set SOURCE_NAME=%~2
set DB_CHOICE=%~3
set TEMPLATE_PATH=%~4

set DB1_PATH=C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated.sqlite
set DB2_PATH=C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated_2.sqlite

REM Determine which database to use
if "%DB_CHOICE%"=="1" (
    set PRODUCTION_DB=%DB1_PATH%
    set DB_NAME=Current Database (db1)
) else if "%DB_CHOICE%"=="2" (
    set PRODUCTION_DB=%DB2_PATH%
    set DB_NAME=New Database (db2)
) else (
    set PRODUCTION_DB=
    set DB_NAME=Will prompt for selection
)

echo ================================================================
echo OCTOPHI INGESTION TO PRODUCTION
echo ================================================================
echo Input File: %INPUT_FILE%
echo Source Name: %SOURCE_NAME%
if defined PRODUCTION_DB (
    echo Database: %DB_NAME%
    echo Path: %PRODUCTION_DB%
) else (
    echo Database: %DB_NAME%
)
if defined TEMPLATE_PATH (
    echo Template: %TEMPLATE_PATH% (Preprocessing Mode)
) else (
    echo Template: None (Fuzzy Mode)
)
echo ================================================================
echo.

call venv\Scripts\activate.bat

REM Build command based on provided parameters
if defined PRODUCTION_DB (
    if defined TEMPLATE_PATH (
        REM With both DB and template
        python -m ingestion.cli ^
            --schema monet ^
            --file "%INPUT_FILE%" ^
            --db "%PRODUCTION_DB%" ^
            --source "%SOURCE_NAME%" ^
            --mapping-template "%TEMPLATE_PATH%" ^
            --create-indexes
    ) else (
        REM With DB only
        python -m ingestion.cli ^
            --schema monet ^
            --file "%INPUT_FILE%" ^
            --db "%PRODUCTION_DB%" ^
            --source "%SOURCE_NAME%" ^
            --create-indexes
    )
) else (
    if defined TEMPLATE_PATH (
        REM With template only (will prompt for DB)
        python -m ingestion.cli ^
            --schema monet ^
            --file "%INPUT_FILE%" ^
            --source "%SOURCE_NAME%" ^
            --mapping-template "%TEMPLATE_PATH%" ^
            --create-indexes
    ) else (
        REM No DB or template (will prompt for DB)
        python -m ingestion.cli ^
            --schema monet ^
            --file "%INPUT_FILE%" ^
            --source "%SOURCE_NAME%" ^
            --create-indexes
    )
)

echo.
echo ================================================================
echo Ingestion complete!
echo ================================================================

pause




