@echo off
if "%~1"=="" (
  echo Usage: push_to_github.bat https://github.com/OWNER/calr-guard.git
  exit /b 1
)
set REMOTE_URL=%~1
if not exist .git (
  git init -b main
  git add .
  git commit -m "Initial commit: CalR Guard 0.3.1"
)
git remote get-url origin >nul 2>nul
if errorlevel 1 (
  git remote add origin %REMOTE_URL%
) else (
  git remote set-url origin %REMOTE_URL%
)
git push -u origin main
