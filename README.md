# Real Time Stock Market Prediction Using Tweet Sentiment Analysis

## Set Up

Run the commands:

```
git clone https://github.com/OmarAtyqy/stock-prediction-using-sentiment-analysis
cd stock-prediction-using-sentiment-analysis
```

Create an `.env` containg the credentials to a MongoDB cluster that has the following:

- Database named **main**
- Collection within this database named **tweets**

```
DB_USERNAME = _______________ # cluster username
DB_PASSWORD = _______________ # cluster password
```

### Windows

Run the `./build.bat` script

### Unix-based systems

Run the `./build.sh` script

## Running the services

Run the following command to start the data fetching service:

```
docker-compose exec -it kafka bash
cd /mnt && python3 -m src.kafka.main <stock_name> <interval>
```

where `<stock_name>` is the stock symbol you want to fetch data for (make sure that it's available in the database) and `<interval>` is the amount of time to wait in-between data fetching.

In another terminal, run the following commands to start the prediction service:

```
docker-compose exec -it dash-app bash
cd /mnt && python3 -m src.spark.main_dashboard
```

In another terminal, run the following commands to start the dashboard service:

```
docker-compose exec -it spark-master bash
cd /mnt && python3 -m src.spark.main_pipeline
```

## Dashboard

...
