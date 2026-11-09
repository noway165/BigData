@echo off
rem Nhap dup de tat demo: dung job Spark roi xoa container va network.
rem Volume duoc giu lai, nen lan sau khong phai format HDFS hay tai lai jar.
setlocal
cd /d "%~dp0"
title Wikipedia Realtime Analytics - stop

echo.
echo Dung job Spark...
podman exec -i spark sh -lc "pkill -9 -f '[a]nalytics_job' || true; pkill -9 -f '[r]aw_to_hdfs' || true" >nul 2>&1

echo Xoa container va network...
podman compose --profile producer --profile dashboard down

echo.
echo Da tat. Volume du lieu van con:
podman volume ls --format "{{.Name}}" | findstr /i wikipedia-realtime-analytics
echo.
pause
