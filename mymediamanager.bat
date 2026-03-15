@echo off
title MyMediaManager - Twin-Engine Media Pipeline
cd /d "E:\CODE\MyMediaManager"

echo [mymediamanager] Starting MyMediaManager Stack...

REM --- Engine A: Series Pipeline ---
start /b python bin\automouse.py --mode series
start /b python bin\autoharbor.py --mode series
start /b python bin\autorouter.py --mode series
start /b python bin\structpilot.py --mode series
start /b python bin\contentclassifier.py --mode series

REM --- Engine B: Movie Pipeline ---
start /b python bin\automouse.py --mode movies
start /b python bin\autoharbor.py --mode movies
start /b python bin\autorouter.py --mode movies
start /b python bin\structpilot.py --mode movies
start /b python bin\movieprocessor.py --mode movies

REM --- Final Processors ---
start /b python bin\seriesprocessor.py --type tv
start /b python bin\seriesprocessor.py --type cartoons
start /b python bin\animeprocessor.py

echo [mymediamanager] All services started. Press Ctrl+C to stop.
pause >nul
