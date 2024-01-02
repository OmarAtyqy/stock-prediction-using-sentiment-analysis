import dash
from dash import dcc, html, callback_context, dash_table
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
        self.interval = 2
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

        self.data = pd.DataFrame(columns=['Date', 'Tweet', 'Low', 'High', 'Open', 'Close', 'Volume', 'Prediction', 'Sentiment'])
        
        self.app = dash.Dash(__name__)
        self.enhance_app_layout()
        

    def enhance_app_layout(self):
        # Styles
        beige_background = {'backgroundColor': '#e3e3de', 'padding': '20px', 'borderRadius': '5px'}
        style_card = {'background': '#f2f2ed', 'borderRadius': '5px', 'boxShadow': '2px 2px 2px lightgrey', 'padding': '15px', 'margin': '10px'}
        style_counter = {'border': '2px solid #4CAF50', 'borderRadius': '50%', 'color': '#4CAF50', 'fontSize': '36px', 'fontWeight': 'bold', 'width': '120px', 'height': '120px', 'textAlign': 'center', 'lineHeight': '120px', 'margin': '0 auto'}
        style_counter_container = {'display': 'flex', 'flexDirection': 'column', 'justifyContent': 'center', 'height': '100%', 'marginRight': '20px'}
        style_counter_title = {'textAlign': 'center', 'fontWeight': 'bold', 'color': '#333'}

        # Dashboard layout
        self.app.layout = html.Div([
            html.H1("Real-Time Stock Prediction Dashboard", style={'textAlign': 'center', 'color': '#333', 'marginBottom': '40px'}),
            self.create_counters_row(style_card, style_counter, style_counter_title, style_counter_container, beige_background),
            self.create_analysis_row(style_card, beige_background),
            dcc.Interval(id='interval-component', interval=self.interval * 1000, n_intervals=0)
        ])
        
        @self.app.callback(
            [Output('sentiment-analysis-graph', 'figure'),
            Output('latest-tweets-display', 'children'),
            Output('tweets-scrapped-display', 'children'),
            Output('mean-prediction-display', 'children'),
            Output('open-display', 'children'),
            Output('low-display', 'children'),
            Output('high-display', 'children'),
            Output('volume-display', 'children')],
            [Input('interval-component', 'n_intervals')])
        def update_content(n):
            # Update logic for each component
            sentiment_fig = self.create_sentiment_analysis_figure()
            latest_tweets_display = self.create_latest_tweets_display()
            tweets_scrapped_display = self.create_display_value(len(self.data), style_counter)
            mean_prediction_display = self.create_display_value(round(self.data['Prediction'].mean(), 2) if len(self.data[self.data['Prediction'].notnull()]) > 0 else 0, style_counter)
            open_display = self.create_display_value(round(self.data['Open'].values[0], 2) if len(self.data) > 0 else 0, style_counter)
            low_display = self.create_display_value(round(self.data['Low'].min(), 2) if len(self.data) > 0 else 0, style_counter)
            high_display = self.create_display_value(round(self.data['High'].max(), 2) if len(self.data) > 0 else 0, style_counter)
            volume_display = self.create_display_value(round(self.data['Volume'].max() / 1_000_000, 2) if len(self.data) > 0 else 0, style_counter)

            return sentiment_fig, latest_tweets_display, tweets_scrapped_display, mean_prediction_display, open_display, low_display, high_display, volume_display

    def create_counters_row(self, style_card, style_counter, style_counter_title, style_counter_container, beige_background):
        # Use `justifyContent: 'space-around'` to give space around each item
        # or `justifyContent: 'space-between'` to create equal space between the items.
        counters_style = {'display': 'flex', 'justifyContent': 'space-around', **beige_background}
        return html.Div([
            self.create_counter("Tweets Scrapped", 'tweets-scrapped-display', style_counter, style_counter_title, style_counter_container),
            self.create_counter("Open", 'open-display', style_counter, style_counter_title, style_counter_container),
            self.create_counter("Low", 'low-display', style_counter, style_counter_title, style_counter_container),
            self.create_counter("High", 'high-display', style_counter, style_counter_title, style_counter_container),
            self.create_counter("Volume", 'volume-display', style_counter, style_counter_title, style_counter_container),
            self.create_counter("Prediction", 'mean-prediction-display', style_counter, style_counter_title, style_counter_container)
        ], style=counters_style)


    def create_analysis_row(self, style_card, beige_background):
        return html.Div([
            html.Div([dcc.Graph(id='sentiment-analysis-graph')], style={'flex': '1', 'padding': '0 20px', **style_card}),
            html.Div([
                html.H4("Latest Tweets", style={'textAlign': 'center'}),
                html.Div(id='latest-tweets-display', style={
                    'overflowY': 'scroll', 'maxHeight': '400px', 'border': '1px solid #dcdcdc', 'borderRadius': '5px', 'marginTop': '10px', 'padding': '10px', 'backgroundColor': 'white'
                })
            ], style={'flex': '1', 'padding': '0 20px', **style_card})
        ], style={'display': 'flex', 'flexWrap': 'wrap', 'justifyContent': 'space-around', **beige_background})

    def create_counter(self, title, id, style_counter, style_counter_title, style_counter_container):
        return html.Div([
            html.Div([html.H4(title, style=style_counter_title), html.Div(id=id, style=style_counter)], style=style_counter_container)
        ])

    def create_sentiment_analysis_figure(self):
        sentiment_counts = self.data['Sentiment'].value_counts()
        sentiment_colors = {'Positive': 'green', 'Negative': 'red', 'Neutral': 'blue'}
        sentiment_fig = px.bar(sentiment_counts, labels={'index': 'Sentiment', 'value': 'Count'}, color=sentiment_counts.index.map(sentiment_colors))
        sentiment_fig.update_layout(title={'text': 'Tweet Sentiment Analysis', 'y':0.95, 'x':0.5, 'xanchor': 'center', 'yanchor': 'top'})
        return sentiment_fig

    def create_latest_tweets_display(self):
        latest_tweets = self.data['Tweet'].tail(10)
        return html.Ul([html.Li(tweet) for tweet in latest_tweets])

    def create_display_value(self, value, style_counter):
        return html.Div(html.Div(f"{value}", style=style_counter), style={'margin': '0 auto', 'width': 'fit-content'})


    def _fetch_tweet_data(self):
        while True:
            tweet_messages = self.tweets_consumer.poll(timeout_ms=1000)  # Poll for 1 second
            for topic_partition, messages in tweet_messages.items():
                for message in messages:
                    tweet_data = message.value
                    self.tweets_queue.put(tweet_data)

    def _fetch_prediction_data(self):
        while True:
            prediction_messages = self.predictions_consumer.poll(timeout_ms=1000)  # Poll for 1 second
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
                    new_tweet_data = new_tweet_data.reindex(columns=self.data.columns)
                    self.data = pd.concat([self.data, new_tweet_data], ignore_index=True)
                    self.data['Date'] = pd.to_datetime(self.data['Date'])
                    self.data = self.data.sort_values(by=['Date'], ascending=False)
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
                    new_prediction_data['Date'] = pd.to_datetime(new_prediction_data['Date'])
                    self.data['Date'] = pd.to_datetime(self.data['Date'])

                    if 'Prediction' in new_prediction_data.columns and 'Date' in new_prediction_data.columns:
                        for _, row in new_prediction_data.iterrows():
                            # Find matching indices
                            indices = self.data[(self.data['Date'] == row['Date']) & (self.data['Tweet'] == row['Tweet'])].index
                            for index in indices:
                                self.data.at[index, 'Prediction'] = row['Prediction']
                                if row['Prediction'] > row['Close'] * 1.0001:
                                    self.data.at[index, 'Sentiment'] = 'Positive'
                                elif row['Prediction'] < row['Close'] * 0.9999:
                                    self.data.at[index, 'Sentiment'] = 'Negative'
                                else:
                                    self.data.at[index, 'Sentiment'] = 'Neutral'
                    else:
                        print("Missing required columns in prediction data")
                except Exception as e:
                    print("Error processing prediction data:", e)
                self.predictions_queue.task_done()

            time.sleep(self.interval)
            print("Done updating data, new length:", len(self.data), "new prediction count:", len(self.data[self.data['Prediction'].notnull()]))



    def _run(self):
        # Thread to fetch tweet data
        thread_fetch_tweet_data = Thread(target=self._fetch_tweet_data)
        thread_fetch_tweet_data.start()

        # Thread to fetch prediction data
        thread_fetch_prediction_data = Thread(target=self._fetch_prediction_data)
        thread_fetch_prediction_data.start()

        # Thread to update data
        thread_update_data = Thread(target=self.update_data)
        thread_update_data.start()

        # Thread to run the Dash server
        dash_thread = Thread(target=lambda: self.app.run_server(host='0.0.0.0'))
        dash_thread.start()

    def start(self):
        print("Starting PredictionConsumer for topic: ", self._prediction_topic_name)
        self._run()

