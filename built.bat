@echo off
REM Stop the current running containers
docker-compose down

REM Build the new image with the updated requirements
docker build -t stock-prediction-spark .

REM Start the services with the new image
docker-compose up --build -d
