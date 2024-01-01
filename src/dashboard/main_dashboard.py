from ..services.prediction_consumer import PredictionConsumerWithDashboard


def main():

    # create the producer
    consumer = PredictionConsumerWithDashboard()

    # start the producer
    consumer.start()


if __name__ == "__main__":
    main()
