@echo off
REM Batch Ingestion Wrapper for Windows
REM Usage: batch_ingest.bat <template_path> <file_pattern_or_files>

setlocal enabledelayedexpansion

set PYTHON=C:\Users\ottog\Desktop\monet_database_engine\venv\Scripts\python.exe

if "%1"=="" (
    echo Usage: batch_ingest.bat ^<template_path^> ^<file_pattern_or_files^>
    echo.
    echo Examples:
    echo   batch_ingest.bat template.csv "C:\folder\*.csv"
    echo   batch_ingest.bat template.csv file1.csv file2.csv file3.csv
    exit /b 1
)

set TEMPLATE=%1
shift

REM Collect all remaining arguments as files
set FILES=
:loop
if "%1"=="" goto endloop
set FILES=!FILES! %1
shift
goto loop
:endloop

echo Starting batch ingestion...
%PYTHON% batch_ingest.py --template %TEMPLATE% --files %FILES%
