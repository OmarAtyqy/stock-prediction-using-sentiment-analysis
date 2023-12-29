from kafka import KafkaProducer
import json
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from time import sleep
from dotenv import load_dotenv


class TweetProducer:
    """
    Producer class to send tweet and stock data to kafka topic.
    """

    def __init__(self, stock_name, interval=None):

        self._topic_name = "stock-tweets"
        self._stock_name = stock_name
        if interval is None:
            self._interval = 10
        else:
            self._interval = interval

        # get the environment variables
        load_dotenv()
        mongo_user = os.getenv("DB_USERNAME")
        mongo_pass = os.getenv("DB_PASSWORD")
        if mongo_user is None or mongo_pass is None:
            raise ValueError("Please set the environment variables")

        # connect to mongodb
        uri = f"mongodb+srv://{mongo_user}:{mongo_pass}@cluster0.jaicils.mongodb.net/?retryWrites=true&w=majority"
        self._client = MongoClient(uri, server_api=ServerApi('1'))
        self._db = self._client['main']
        self._collection = self._db['tweets']

        # connect to kafka
        self._producer = KafkaProducer(
            bootstrap_servers=['localhost:9093'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    def _fetch_data(self, n_records=100):
        """
        Fetch n_records from mongodb randomly.
        """

        # fetch n_records from mongodb where "Stock Name" equals self._stock_name
        cursor = self._collection.aggregate([
            {"$match": {"Stock Name": self._stock_name}},
            {"$sample": {"size": n_records}}
        ])

        # return the cursor
        return cursor

    def _send_data(self, data):
        """
        Send data to Kafka topic.
        """
        try:
            for record in data:
                # remove the _id field since it is not serializable
                del record['_id']
                self._producer.send(self._topic_name, value=record)

            self._producer.flush()
        finally:
            # Ensure the cursor is closed properly
            data.close()

    def _run(self):
        """
        Run the producer.
        """

        while True:

            # fetch data from mongodb
            print("Fetching data...")
            data = self._fetch_data()

            # send data to kafka topic
            print("Sending data...")
            self._send_data(data)

            # sleep
            print(f"Sleeping for {self._interval} seconds...")
            sleep(self._interval)

    def start(self):
        """
        Start the producer.
        """
        self._run()
