@echo off
REM Stop the current running containers
docker-compose down

REM Remove dangling images
docker image prune -f

REM Build the new image with the updated requirements
docker build -f Docker/spark/Dockerfile -t stock-prediction-spark .
docker build -f Docker/kafka/Dockerfile -t stock-prediction-kafka .

REM Start the services with the new image
docker-compose up --build -d
