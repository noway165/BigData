@echo off
rem Nhap dup de chay toan bo demo: Podman, Kafka, HDFS, Spark, producer, job
rem analytics va dashboard. Chay lai duoc nhieu lan, khong tao trung job.
rem Khong dau tieng Viet: CMD doc file bat theo code page he thong.
setlocal
cd /d "%~dp0"
title Wikipedia Realtime Analytics - demo
set "DOCKER=podman"
set "STALE_SOCK=%TEMP%\podman\podman-machine-default-api.sock"

where podman >nul 2>&1
if errorlevel 1 (
  echo [LOI] Khong tim thay podman trong PATH.
  echo       Cai Podman Desktop roi chay lai file nay.
  pause
  exit /b 1
)

echo.
echo [1/5] Khoi dong Podman machine...
podman machine start >nul 2>&1
rem win-sshproxy.exe chet neu con file socket cu, va khi do compose bao loi npipe.
rem Thu goi compose that: do la thu duy nhat biet chac API da ket noi duoc.
podman compose ls >nul 2>&1
if errorlevel 1 (
  echo       Chua ket noi duoc docker API, xoa socket cu roi thu lai...
  del /f /q "%STALE_SOCK%" >nul 2>&1
  podman machine stop >nul 2>&1
  podman machine start >nul 2>&1
)
podman compose ls >nul 2>&1
if errorlevel 1 (
  echo [LOI] Khong ket noi duoc docker API cua Podman.
  echo       Thu thu cong: podman machine stop ^&^& podman machine start
  pause
  exit /b 1
)

echo.
echo [2/5] Bat Kafka, HDFS, Spark, producer, dashboard...
podman compose --profile producer --profile dashboard up -d
if errorlevel 1 (
  echo [LOI] compose that bai. Xem thong bao ben tren.
  pause
  exit /b 1
)

echo.
echo [3/5] Tao topic va thu muc HDFS...
set "GITBASH=%ProgramFiles%\Git\bin\bash.exe"
if exist "%GITBASH%" (
  "%GITBASH%" scripts/bootstrap.sh
) else (
  echo       Khong tim thay Git Bash, bo qua buoc nay.
  echo       Neu topic chua ton tai, chay tay: DOCKER=podman sh scripts/bootstrap.sh
)

echo.
echo [4/5] Chay job analytics trong container spark...
rem Pattern [a]nalytics_job de pgrep khong khop chinh cmdline cua lenh kiem tra.
podman exec -d spark sh -lc "pgrep -f '[a]nalytics_job' >/dev/null || sh /opt/app/scripts/submit_analytics_job.sh > /opt/app/outputs/analytics_demo.log 2>&1"

echo.
echo [5/5] Mo dashboard...
start "" http://localhost:8080

echo.
echo ================================================================
echo  Dashboard : http://localhost:8080   (tu refresh 5 giay)
echo  NameNode  : http://localhost:9870
echo  Spark UI  : http://localhost:4040
echo.
echo  Trang hien ngay du lieu con trong retention 24 gio.
echo  Du lieu MOI can khoang 2 phut (window 1 phut + watermark 1 phut),
echo  rieng bot_ratio can khoang 6 phut vi window 5 phut.
echo.
echo  Tat tat ca: nhap dup stop_demo.bat
echo ================================================================
echo.
pause
