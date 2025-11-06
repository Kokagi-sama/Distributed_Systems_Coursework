@echo off
echo Starting Raft Cluster...

:: Start Server 1 in a new window
start "Server 1 (Port 50051)" cmd /k python server/FileServer.py --id S1 --port 50051 --peers "localhost:50052,localhost:50053"

:: Wait 2 seconds to let the first one initialize
timeout /t 2 /nobreak >nul

:: Start Server 2 in a new window
start "Server 2 (Port 50052)" cmd /k python server/FileServer.py --id S2 --port 50052 --peers "localhost:50051,localhost:50053"

:: Wait 1 second
timeout /t 1 /nobreak >nul

:: Start Server 3 in a new window
start "Server 3 (Port 50053)" cmd /k python server/FileServer.py --id S3 --port 50053 --peers "localhost:50051,localhost:50052"

echo.
echo Cluster started!
echo You should see three new windows, one for each server.
echo To stop the cluster, just close those three windows.
pause