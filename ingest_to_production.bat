@echo off
REM Quick ingestion script for production database
REM Usage: ingest_to_production.bat "path\to\file.csv" "Source_Name"

setlocal

if "%~1"=="" (
    echo Usage: ingest_to_production.bat "path\to\file.csv" "Source_Name"
    echo.
    echo Example:
    echo   ingest_to_production.bat "..\leads.csv" "BrokerName"
    exit /b 1
)

if "%~2"=="" (
    echo Error: Source name is required
    echo Usage: ingest_to_production.bat "path\to\file.csv" "Source_Name"
    exit /b 1
)

set INPUT_FILE=%~1
set SOURCE_NAME=%~2
set PRODUCTION_DB=C:\Users\ottog\Desktop\monet_database_engine\unified_database_migrated.sqlite

echo ================================================================
echo OCTOPHI INGESTION TO PRODUCTION
echo ================================================================
echo Input File: %INPUT_FILE%
echo Source Name: %SOURCE_NAME%
echo Database: %PRODUCTION_DB%
echo ================================================================
echo.

call venv\Scripts\activate.bat

python -m ingestion.cli ^
    --schema monet ^
    --file "%INPUT_FILE%" ^
    --db "%PRODUCTION_DB%" ^
    --source "%SOURCE_NAME%" ^
    --create-indexes

echo.
echo ================================================================
echo Ingestion complete!
echo ================================================================

pause




