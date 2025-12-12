#!/usr/bin/env pwsh
<#
PowerShell script to setup and run the chatbot
Run this from the projectubs directory
#>

Write-Host ""
Write-Host "========================================"
Write-Host "  JEWELRY SALES CHATBOT SETUP"
Write-Host "========================================"
Write-Host ""

# Check if Python is installed
try {
    $pythonVersion = python --version 2>&1
    Write-Host "✓ Found: $pythonVersion"
} catch {
    Write-Host "❌ ERROR: Python is not installed or not in PATH"
    exit 1
}

Write-Host "[1/5] Creating virtual environment..."
if (-not (Test-Path venv)) {
    python -m venv venv
    Write-Host "✓ Virtual environment created"
} else {
    Write-Host "✓ Virtual environment already exists"
}

Write-Host "[2/5] Activating virtual environment..."
& .\venv\Scripts\Activate.ps1

Write-Host "[3/5] Installing dependencies..."
pip install -r requirements.txt -q
Write-Host "✓ Dependencies installed"

Write-Host "[4/5] Running data pipeline..."
Write-Host "   - Cleaning data..."
python data\cleaning.py
Write-Host "   - Initializing database..."
python data\init_db.py

Write-Host "[5/5] Starting API server..."
Write-Host ""
Write-Host "========================================"
Write-Host "  SERVER STARTING AT: http://localhost:8000"
Write-Host "  DOCUMENTATION: http://localhost:8000/docs"
Write-Host "========================================"
Write-Host ""

python -m uvicorn app.main:app --reload
