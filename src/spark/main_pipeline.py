from ..services.tweet_consumer_prediction_producer import TweetConsumerPredictionProducer


def main():

    # create the producer
    consumer = TweetConsumerPredictionProducer()

    # start the producer
    consumer.start()


if __name__ == "__main__":
    main()
