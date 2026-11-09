@echo off
rem Nhap dup de chay toan bo demo, roi nhan Q de tat.
rem Bat Podman, Kafka, HDFS, Spark, producer, job analytics, Dashboard va Live feed.
rem Chay lai duoc nhieu lan, khong tao trung job.
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
podman compose ps >nul 2>&1
if errorlevel 1 (
  echo       Chua ket noi duoc docker API, xoa socket cu roi thu lai...
  del /f /q "%STALE_SOCK%" >nul 2>&1
  podman machine stop >nul 2>&1
  podman machine start >nul 2>&1
)
podman compose ps >nul 2>&1
if errorlevel 1 (
  echo [LOI] Khong ket noi duoc docker API cua Podman.
  echo       Thu thu cong: podman machine stop ^&^& podman machine start
  pause
  exit /b 1
)

echo.
echo [2/5] Bat Kafka, HDFS, Spark, producer, Dashboard, Live feed...
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
echo [4/5] Chay job raw va analytics trong container spark...
rem Mau phai co .py: neu khong, pgrep khop chu submit_analytics_job.sh trong chinh lenh nay va bo qua job.
podman exec -d spark sh -lc "pgrep -f '[r]aw_to_hdfs' >/dev/null || sh /opt/app/scripts/submit_raw_job.sh > /opt/app/outputs/raw_demo.log 2>&1"
rem Hai spark-submit cung luc co the chan nhau o buoc tai jar, nen cho job dau chay truoc.
ping -n 21 127.0.0.1 >nul
rem edit,new: chi dem luot sua that, bang dot bien khong bi tran trang Category.
podman exec -d -e ANALYTICS_EVENT_TYPES=edit,new spark sh -lc "pgrep -f '[a]nalytics_job[.]py' >/dev/null || sh /opt/app/scripts/submit_analytics_job.sh > /opt/app/outputs/analytics_demo.log 2>&1"

echo.
echo [5/5] Mo trinh duyet...
start "" http://localhost:8501
start "" http://localhost:8080

:menu
echo.
echo ================================================================
echo  Dashboard : http://localhost:8501   (Streamlit)
echo  Live feed : http://localhost:8080   (log tung luot sua)
echo  NameNode  : http://localhost:9870
echo  Spark UI  : http://localhost:4040
echo.
echo  Trang hien ngay du lieu con trong retention 24 gio.
echo  Du lieu MOI can khoang 2 phut (window 1 phut + watermark 1 phut),
echo  rieng bot_ratio can khoang 6 phut vi window 5 phut.
echo.
echo  [Q] Tat tat ca     [R] Mo lai trinh duyet     [S] Xem trang thai
echo ================================================================
choice /c QRS /n /m "Chon: "
if errorlevel 3 goto status
if errorlevel 2 goto reopen
goto confirmstop

:reopen
start "" http://localhost:8501
start "" http://localhost:8080
goto menu

:status
echo.
podman ps --format "table {{.Names}}\t{{.Status}}"
goto menu

:confirmstop
echo.
echo  Sap tat toan bo stack. Dang trinh bay thi dung tat.
choice /c YN /n /m "Chac chan tat? [Y/N] "
if errorlevel 2 goto menu

echo.
echo Dung job Spark...
podman exec -i spark sh -lc "pkill -9 -f '[a]nalytics_job' || true; pkill -9 -f '[r]aw_to_hdfs' || true" >nul 2>&1

echo Xoa container va network...
podman compose --profile producer --profile dashboard down

echo.
echo Da tat. Volume du lieu van con, nen lan sau khong phai format HDFS
echo hay tai lai jar connector:
podman volume ls --format "{{.Name}}" | findstr /i wikipedia-realtime-analytics
echo.
pause
