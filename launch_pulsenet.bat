@echo off
setlocal
cd /d "%~dp0"

set "PYTHON_EXE=python"
if exist "%~dp0.venv\Scripts\python.exe" set "PYTHON_EXE=%~dp0.venv\Scripts\python.exe"
REM Prefer the usual dashboard port, but use 8101 if an older PulseNet
REM instance is still occupying 8100. This prevents opening the old dashboard.
set "PULSENET_DASHBOARD_PORT=8100"
"%PYTHON_EXE%" -c "import socket,sys;s=socket.socket();s.settimeout(.3);r=s.connect_ex(('127.0.0.1',8100));s.close();sys.exit(0 if r==0 else 1)" >nul 2>&1
if not errorlevel 1 set "PULSENET_DASHBOARD_PORT=8101"

echo Starting PulseNet dashboard on http://127.0.0.1:%PULSENET_DASHBOARD_PORT%
echo Node ports remain 8001, 8002, 8003, and so on.
echo.

"%PYTHON_EXE%" -c "import fastapi, uvicorn, httpx" >nul 2>&1
if errorlevel 1 (
    echo Required Python packages are missing. Installing them now...
    "%PYTHON_EXE%" -m pip install -r requirements.txt
    if errorlevel 1 (
        echo Could not install dependencies. Check your Python installation and internet connection.
        pause
        exit /b 1
    )
)

start "PulseNet Dashboard Server" /min cmd /k ""%PYTHON_EXE%" -m uvicorn main:app --host 127.0.0.1 --port %PULSENET_DASHBOARD_PORT%"
timeout /t 3 /nobreak >nul
start "" "http://127.0.0.1:%PULSENET_DASHBOARD_PORT%"
endlocal
