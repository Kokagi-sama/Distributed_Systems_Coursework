@echo off
REM Batch file to start 4 client instances
REM Usage: start_clients.bat

REM Navigate to project root (2 levels up from client/scripts)
cd /d "%~dp0..\.."

REM Store the project root path
set PROJECT_ROOT=%CD%

REM Set your coordinator and fileserver addresses here
set COORDINATORS=localhost:60051 localhost:60052
set FILESERVERS=localhost:50051 localhost:50052 localhost:50053

echo Starting 4 client instances...
echo.

REM Start Client 1
echo Starting Client 1...
start "Client-1" cmd /k "cd /d %PROJECT_ROOT% && python -u client\client.py client_1 --coordinators %COORDINATORS% --fileservers %FILESERVERS%"

REM Wait a bit before starting next client
timeout /t 1 /nobreak >nul

REM Start Client 2
echo Starting Client 2...
start "Client-2" cmd /k "cd /d %PROJECT_ROOT% && python -u client\client.py client_2 --coordinators %COORDINATORS% --fileservers %FILESERVERS%"

REM Wait a bit before starting next client
timeout /t 1 /nobreak >nul

REM Start Client 3
echo Starting Client 3...
start "Client-3" cmd /k "cd /d %PROJECT_ROOT% && python -u client\client.py client_3 --coordinators %COORDINATORS% --fileservers %FILESERVERS%"

REM Wait a bit before starting next client
timeout /t 1 /nobreak >nul

REM Start Client 4
echo Starting Client 4...
start "Client-4" cmd /k "cd /d %PROJECT_ROOT% && python -u client\client.py client_4 --coordinators %COORDINATORS% --fileservers %FILESERVERS%"

echo.
echo All 4 clients started!
echo Each client is running in its own window.
echo.
pause
