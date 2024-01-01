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

## Running the Services

To start the various services of the project, you can either manually run each service or use the provided scripts for convenience.

### Manual Startup

1. **Dashboard Service**:
   ```bash
   docker-compose exec -it dash-app bash
   cd /mnt && python3 -m src.dashboard.main_dashboard
   ```

2. **Prediction Service**:
   ```bash
   docker-compose exec -it spark-master bash
   cd /mnt && python3 -m src.spark.main_pipeline
   ```

3. **Data Fetching Service**:
   ```bash
   docker-compose exec -it kafka bash
   cd /mnt && python3 -m src.kafka.main <stock_name> <interval>
   ```
   Replace `<stock_name>` with the desired stock symbol and `<interval>` with the data fetching interval.

### Automated Startup

Alternatively, you can use the provided scripts to start all services simultaneously:

#### For Windows Users:

Run the `run.bat` script:

```bash
./run.bat <stock_name> <interval>
```

#### For Unix-based System Users:

Run the `run.sh` script:

```bash
./run.sh <stock_name> <interval>
```

In both scripts, replace `<stock_name>` with the stock symbol and `<interval>` with the data fetching interval.
