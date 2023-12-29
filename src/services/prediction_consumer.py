from kafka import KafkaConsumer
import json


class PredictionConsumerWithDashboard:

    def __init__(self):
        """
        Initialize the consumer.
        """
        self._prediction_topic_name = "predictions"

        # connect to kafka
        self._consumer = KafkaConsumer(
            self._prediction_topic_name,
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def _run(self):
        """
        Run the consumer.
        """
        for message in self._consumer:

            # get the data and print out the following columns: Date, Tweet, Low, High, Open, Close, Adj Close, Volume, Prediction
            data = message.value
            print("Date of the prediction: ", data["Date"])
            print("Tweet: ", data["Tweet"])
            print("Low: ", data["Low"])
            print("High: ", data["High"])
            print("Open: ", data["Open"])
            print("Close: ", data["Close"])
            print("Adj Close: ", data["Adj Close"])
            print("Volume: ", data["Volume"])
            print("Predicted close: ", data["Prediction"])

            #  DASHBOARD CODE GOES HERE
            #
            #
            #
            # # # # # # # # # #

    def start(self):
        """
        Start the consumer.
        """
        print("Starting PredictionConsumer for topic: ",
              self._prediction_topic_name)
        self._run()
