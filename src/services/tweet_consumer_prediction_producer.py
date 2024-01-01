from kafka import KafkaConsumer, KafkaProducer
import json
from pyspark.sql import SparkSession
from ..models.linear_regression import RegressionModel
import time


class TweetConsumerPredictionProducer:

    def __init__(self):
        """
        Initialize the consumer.
        """
        self._data_topic_name = "stock-tweets"
        self._prediction_topic_name = "predictions"

        self._consumer = KafkaConsumer(
            self._data_topic_name,
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='latest',  # Change to latest
            enable_auto_commit=True,
            group_id='group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        # create the kafka producer for predictions
        self._producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

        # create a spark session
        self._spark = SparkSession.builder \
            .appName("stock-prediction") \
            .getOrCreate()

        # load the model
        self._model = RegressionModel(
            preprocessing_pipeline_path="saved_models/preprocessing_pipeline",
            model_path="saved_models/final_model"
        )

    def _run(self):
        """
        Run the consumer.
        """

        while True:
            messages = self._consumer.poll(
                timeout_ms=1000)  # Poll for 1 second
            for topic_partition, msg_list in messages.items():
                for message in msg_list:
                    # get the data
                    data = message.value

                    # create a spark dataframe
                    data_df = self._spark.createDataFrame([data])

                    # make prediction
                    prediction = self._model(data_df)

                    # add the prediction to the data
                    data["Prediction"] = prediction.collect()[0]["prediction"]

                    print("Sending prediction:", data)

                    # send the data to the prediction topic
                    self._producer.send(
                        self._prediction_topic_name, value=data)

            # Sleep for a short duration to avoid constant polling
            time.sleep(1)

    def start(self):
        """
        Start the consumer.
        """
        print("Starting TweetConsumerPredictionProducer for topic: ",
              self._data_topic_name)
        self._run()
