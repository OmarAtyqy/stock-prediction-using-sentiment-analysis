@echo off

:: Check if stock name and interval are provided
IF "%~1"=="" GOTO USAGE
IF "%~2"=="" GOTO USAGE

SET STOCK_NAME=%1
SET INTERVAL=%2

:: Start the dashboard service in the background
START /B CMD /C "docker-compose exec -d dash-app bash -c "cd /mnt && python3 -m src.dashboard.main_dashboard""
TIMEOUT /T 10 /NOBREAK

:: Start the prediction service in the background
START /B CMD /C "docker-compose exec -d spark-master bash -c "cd /mnt && python3 -m src.spark.main_pipeline""
TIMEOUT /T 10 /NOBREAK

:: Start the data fetching service
docker-compose exec -it kafka bash -c "cd /mnt && python3 -m src.kafka.main %STOCK_NAME% %INTERVAL%"

GOTO END

:USAGE
echo Usage: run.bat <stock_name> <interval>
GOTO END

:END
