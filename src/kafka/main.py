from ..services.tweet_producer import TweetProducer
import sys


def main():

    # check the number of arguments
    if len(sys.argv) < 2:
        print("Usage: python3 -m src.kafka.main <stock_name> <interval (optional)>")
        return

    # get the stock name
    stock_name = sys.argv[1]

    # get the interval
    interval = None
    if len(sys.argv) == 3:
        interval = int(sys.argv[2])

    # create the producer
    producer = TweetProducer(stock_name, interval)

    # start the producer
    producer.start()


if __name__ == "__main__":
    main()
