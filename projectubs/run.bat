@echo off
REM Windows batch script to setup and run the chatbot
REM Run this from the projectubs directory

echo.
echo ========================================
echo  JEWELRY SALES CHATBOT SETUP
echo ========================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH
    exit /b 1
)

echo [1/5] Creating virtual environment...
if not exist venv (
    python -m venv venv
) else (
    echo Virtual environment already exists
)

echo [2/5] Activating virtual environment...
call venv\Scripts\activate.bat

echo [3/5] Installing dependencies...
pip install -r requirements.txt -q

echo [4/5] Running data pipeline...
echo   - Cleaning data...
python data/cleaning.py
echo   - Initializing database...
python data/init_db.py

echo [5/5] Starting API server...
echo.
echo ========================================
echo  SERVER STARTING AT: http://localhost:8000
echo  DOCUMENTATION: http://localhost:8000/docs
echo ========================================
echo.

python -m uvicorn app.main:app --reload

pause
