import dash
from dash import dcc, html, callback_context
from dash.dependencies import Output, Input, State
from plotly.subplots import make_subplots
import pandas as pd
import plotly.graph_objs as go
from kafka import KafkaConsumer
import json
from threading import Event, Thread
from queue import Queue
import time
from collections import Counter
import plotly.express as px


class PredictionConsumerWithDashboard:
    def __init__(self):
        self.interval = 5
        self._data_topic_name = "stock-tweets"
        self._prediction_topic_name = "predictions"

        self.tweets_consumer = KafkaConsumer(
            self._data_topic_name,
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='dash_group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        self.predictions_consumer = KafkaConsumer(
            self._prediction_topic_name,
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='group2',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        self.tweets_queue = Queue()
        self.predictions_queue = Queue()
        self.update_event = Event()

        self.data = pd.DataFrame(columns=[
                                 'Date', 'Tweet', 'Low', 'High', 'Open', 'Close', 'Volume', 'Prediction', 'Sentiment'])

        self.app = dash.Dash(__name__)
        self.enhance_app_layout()

    def enhance_app_layout(self):
        # Styles
        style_center_vertically = {'display': 'flex', 'alignItems': 'center'}
        style_counter_container = {
            'display': 'flex', 'flexDirection': 'column', 'justifyContent': 'center', 'height': '100%'}
        style_card = {
            'background': '#f2f2ed', 'borderRadius': '5px', 'boxShadow': '2px 2px 2px lightgrey',
            'padding': '15px', 'margin': '10px'
        }
        beige_background = {'backgroundColor': '#e3e3de',
                            'padding': '20px', 'borderRadius': '5px'}
        style_counter = {
            'border': '2px solid #4CAF50',
            'borderRadius': '50%',  # Makes the div circular
            'color': '#4CAF50',
            'fontSize': '36px',  # Larger font size
            'fontWeight': 'bold',
            'width': '120px',  # Width of the circle
            'height': '120px',  # Height of the circle
            'textAlign': 'center',
            'lineHeight': '120px',  # Centers text vertically
            'margin': '0 auto'
        }
        style_counter_title = {'textAlign': 'center',
                               'fontWeight': 'bold', 'color': '#333'}

        # Layout
        self.app.layout = html.Div([
            html.H1("Real-Time Stock Prediction Dashboard",
                    style={'textAlign': 'center', 'color': '#333', 'marginBottom': '40px'}),

            # First row with Histogram and counters
            html.Div([
                html.Div([
                    dcc.Graph(id='stock-metrics-histogram',
                              style={'height': '400px'}),
                ], style={'flex': '2', **style_card}),

                # Counter displays with titles
                html.Div([
                    html.Div([
                        html.H4("Tweets Scrapped", style=style_counter_title),
                        html.Div(id='tweets-scrapped-display',
                                 style=style_counter),
                    ], style={'marginRight': '20px', **style_counter_container}),  # Add right margin to the first counter
                    html.Div([
                        html.H4("Prediction", style=style_counter_title),
                        html.Div(id='mean-prediction-display',
                                 style=style_counter),
                    ], style=style_counter_container),
                ], style={'flex': '1', 'display': 'flex', 'justifyContent': 'space-evenly'}),

            ], style={'display': 'flex', **beige_background}),

            # Second row with Sentiment Analysis and Latest Tweets Display
            html.Div([
                html.Div([
                    dcc.Graph(id='sentiment-analysis-graph'),
                ], style={'flex': '1', 'padding': '0 20px', **style_card}),

                html.Div([
                    html.H4("Latest Tweets", style={'textAlign': 'center'}),
                    html.Div(id='latest-tweets-display', style={
                        'overflowY': 'scroll',
                        'maxHeight': '400px',
                        'border': '1px solid #dcdcdc',
                        'borderRadius': '5px',
                        'marginTop': '10px',
                        'padding': '10px',
                        'backgroundColor': 'white'
                    }),
                ], style={'flex': '1', 'padding': '0 20px', **style_card})

            ], style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'space-around', **beige_background}),

            dcc.Interval(id='interval-component',
                         interval=self.interval * 1000, n_intervals=0)
        ])

        @self.app.callback(
            [Output('sentiment-analysis-graph', 'figure'),
             Output('latest-tweets-display', 'children'),
             Output('stock-metrics-histogram', 'figure'),
             Output('tweets-scrapped-display', 'children'),
             Output('mean-prediction-display', 'children')],
            [Input('interval-component', 'n_intervals')])
        def update_content(n):
            # Sentiment Analysis Graph
            sentiment_counts = self.data['Sentiment'].value_counts()
            sentiment_colors = {'Positive': 'green',
                                'Negative': 'red', 'Neutral': 'blue'}
            sentiment_fig = px.bar(sentiment_counts,
                                   labels={'index': 'Sentiment',
                                           'value': 'Count'},
                                   color=sentiment_counts.index.map(sentiment_colors))
            sentiment_fig.update_layout(
                title={
                    'text': 'Tweet Sentiment Analysis',
                    'y': 0.95,
                    'x': 0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                }
            )

            # Latest Tweets Display
            latest_tweets = self.data['Tweet'].tail(
                10)  # Adjust number as needed
            latest_tweets_display = html.Ul(
                [html.Li(tweet) for tweet in latest_tweets])

            # Stock Metrics Histogram
            stock_metric_columns = ['Low', 'High', 'Open', 'Close', 'Volume']
            histogram_fig = make_subplots(
                rows=2, cols=3, subplot_titles=stock_metric_columns)
            for i, column in enumerate(stock_metric_columns):
                histogram_fig.add_trace(go.Histogram(
                    x=self.data[column], name=column), row=(i // 3) + 1, col=(i % 3) + 1)

            histogram_fig.update_layout(
                title={
                    'text': 'Stock Metrics Histogram',
                    'y': 0.95,
                    'x': 0.5,
                    'xanchor': 'center',
                    'yanchor': 'top'
                },
                showlegend=False
            )

            # Tweets Scrapped Display with circular and centered styling
            tweets_scrapped_display = html.Div(
                html.Div(f"{len(self.data):,}", style={
                    'border': '2px solid #4CAF50',
                    'borderRadius': '50%',  # Makes the div circular
                    'color': '#4CAF50',
                    'fontSize': '36px',  # Larger font size
                    'fontWeight': 'bold',
                    'width': '120px',  # Width of the circle
                    'height': '120px',  # Height of the circle
                    'textAlign': 'center',
                    'lineHeight': '120px',  # Centers text vertically
                }),
                style={'margin': '0 auto', 'width': 'fit-content'}
            )

            # Mean Prediction Display with circular and centered styling
            mean_prediction = self.data['Prediction'].mean() if len(
                self.data[self.data['Prediction'].notnull()]) > 0 else 0
            mean_prediction_display = html.Div(
                html.Div(f"{mean_prediction:.2f}", style={
                    'border': '2px solid #4CAF50',
                    'borderRadius': '50%',  # Makes the div circular
                    'color': '#4CAF50',
                    'fontSize': '36px',  # Larger font size
                    'fontWeight': 'bold',
                    'width': '120px',  # Width of the circle
                    'height': '120px',  # Height of the circle
                    'textAlign': 'center',
                    'lineHeight': '120px',  # Centers text vertically
                }),
                style={'margin': '0 auto', 'width': 'fit-content'}
            )

            return sentiment_fig, latest_tweets_display, histogram_fig, tweets_scrapped_display, mean_prediction_display

    def _fetch_tweet_data(self):
        while True:
            tweet_messages = self.tweets_consumer.poll(
                timeout_ms=1000)  # Poll for 1 second
            for topic_partition, messages in tweet_messages.items():
                for message in messages:
                    tweet_data = message.value
                    self.tweets_queue.put(tweet_data)

    def _fetch_prediction_data(self):
        while True:
            prediction_messages = self.predictions_consumer.poll(
                timeout_ms=1000)  # Poll for 1 second
            for topic_partition, messages in prediction_messages.items():
                for message in messages:
                    prediction_data = message.value
                    self.predictions_queue.put(prediction_data)

    def update_data(self):
        while True:
            # Process all tweet data items in the queue
            while not self.tweets_queue.empty():
                tweet_data = self.tweets_queue.get()
                try:
                    # Ensure tweet_data is a list of dictionaries
                    if isinstance(tweet_data, dict):
                        tweet_data = [tweet_data]

                    new_tweet_data = pd.DataFrame(tweet_data)
                    # Realign columns to match self.data and handle new incoming tweet data
                    new_tweet_data = new_tweet_data.reindex(
                        columns=self.data.columns)
                    self.data = pd.concat(
                        [self.data, new_tweet_data], ignore_index=True)
                    self.data['Date'] = pd.to_datetime(self.data['Date'])
                    self.data = self.data.sort_values(
                        by=['Date'], ascending=False)
                except Exception as e:
                    print("Error processing tweet data:", e)
                self.tweets_queue.task_done()

            # Process all prediction data items in the queue
            while not self.predictions_queue.empty():
                prediction_data = self.predictions_queue.get()
                try:
                    # Ensure prediction_data is a list of dictionaries
                    if isinstance(prediction_data, dict):
                        prediction_data = [prediction_data]

                    new_prediction_data = pd.DataFrame(prediction_data)

                    # Make sure 'Date' columns are in the same format
                    new_prediction_data['Date'] = pd.to_datetime(
                        new_prediction_data['Date'])
                    self.data['Date'] = pd.to_datetime(self.data['Date'])

                    if 'Prediction' in new_prediction_data.columns and 'Date' in new_prediction_data.columns:
                        for _, row in new_prediction_data.iterrows():
                            # Find matching indices
                            indices = self.data[(self.data['Date'] == row['Date']) & (
                                self.data['Tweet'] == row['Tweet'])].index
                            for index in indices:
                                self.data.at[index,
                                             'Prediction'] = row['Prediction']
                                if row['Prediction'] > row['Close'] * 1.0001:
                                    self.data.at[index,
                                                 'Sentiment'] = 'Positive'
                                elif row['Prediction'] < row['Close'] * 0.9999:
                                    self.data.at[index,
                                                 'Sentiment'] = 'Negative'
                                else:
                                    self.data.at[index,
                                                 'Sentiment'] = 'Neutral'
                    else:
                        print("Missing required columns in prediction data")
                except Exception as e:
                    print("Error processing prediction data:", e)
                self.predictions_queue.task_done()

            time.sleep(self.interval)
            print("Done updating data, new length:", len(self.data), "new prediction count:", len(
                self.data[self.data['Prediction'].notnull()]))

    def _run(self):
        # Thread to fetch tweet data
        thread_fetch_tweet_data = Thread(target=self._fetch_tweet_data)
        thread_fetch_tweet_data.start()

        # Thread to fetch prediction data
        thread_fetch_prediction_data = Thread(
            target=self._fetch_prediction_data)
        thread_fetch_prediction_data.start()

        # Thread to update data
        thread_update_data = Thread(target=self.update_data)
        thread_update_data.start()

        # Thread to run the Dash server
        dash_thread = Thread(
            target=lambda: self.app.run_server(host='0.0.0.0'))
        dash_thread.start()

    def start(self):
        print("Starting PredictionConsumer for topic: ",
              self._prediction_topic_name)
        self._run()
