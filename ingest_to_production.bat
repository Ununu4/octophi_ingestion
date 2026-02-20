@echo off
REM Quick ingestion script for production database
REM Usage: ingest_to_production.bat "path\to\file.csv" "Source_Name" [template_path]
REM   template_path: Required - path to mapping template CSV (incoming_schema,expected_schema)

setlocal

if "%~1"=="" (
    echo Usage: ingest_to_production.bat "path\to\file.csv" "Source_Name" "templates\template.csv"
    echo.
    echo Examples:
    echo   ingest_to_production.bat "..\leads.csv" "BrokerName" "templates\broker.csv"
    echo   ingest_to_production.bat "..\leads.csv" "BrokerName" "" "templates\broker.csv"
    exit /b 1
)

if "%~2"=="" (
    echo Error: Source name is required
    echo Usage: ingest_to_production.bat "path\to\file.csv" "Source_Name" "templates\template.csv"
    exit /b 1
)

set INPUT_FILE=%~1
set SOURCE_NAME=%~2
set TEMPLATE_PATH=%~3

set DEFAULT_DB=postgresql://postgres:postgres@localhost:5433/leadpool_migtest
if not defined DATABASE_URL set DATABASE_URL=%DEFAULT_DB%

set DB_NAME=leadpool_migtest

echo ================================================================
echo OCTOPHI INGESTION TO PRODUCTION
echo ================================================================
echo Input File: %INPUT_FILE%
echo Source Name: %SOURCE_NAME%
echo Database: %DB_NAME%
if defined TEMPLATE_PATH (
    echo Template: %TEMPLATE_PATH%
) else (
    echo Template: REQUIRED - provide path as 4th argument
)
echo ================================================================
echo.

call venv\Scripts\activate.bat

REM Build command (template required for Postgres ingestion)
if defined TEMPLATE_PATH (
    python -m ingestion.cli ^
        --schema monet ^
        --file "%INPUT_FILE%" ^
        --source "%SOURCE_NAME%" ^
        --mapping-template "%TEMPLATE_PATH%"
) else (
    echo ERROR: Template path required. Usage: ingest_to_production.bat "path\to\file.csv" "Source" "templates\source.csv"
    exit /b 1
)

echo.
echo ================================================================
echo Ingestion complete!
echo ================================================================

pause




