import dash
from dash import dcc, html, callback_context
from dash.dependencies import Output, Input, State
from plotly.subplots import make_subplots
import pandas as pd
import plotly.graph_objs as go
from kafka import KafkaConsumer
import json
import threading
from threading import Event
from queue import Queue
import time

class PredictionConsumerWithDashboard:
    def __init__(self):
        self.interval = 10  
        self._prediction_topic_name = "predictions"
        self._consumer = KafkaConsumer(
            self._prediction_topic_name,
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        self.queue = Queue()
        self.update_event = Event()

        self.data = pd.DataFrame(columns=['Date', 'Tweet', 'Low', 'High', 'Open', 'Close', 'Adj Close', 'Volume', 'Prediction'])
        
        self.app = dash.Dash(__name__)
        self.enhance_app_layout()

    def enhance_app_layout(self):
        self.app.layout = html.Div([
            html.H1("Real-Time Stock Prediction Dashboard", style={'textAlign': 'center'}),

            # Subplots and counter container with Flexbox styling
            html.Div([
                html.Div([
                    html.H3("Stock Metrics History", style={'textAlign': 'center'}),
                    dcc.Graph(id='subplot-graph')
                ], style={'flex': '3', 'display': 'flex', 'flexDirection': 'column'}),

                html.Div([
                    html.H3("Fetched Tweets Count", style={'textAlign': 'center'}),
                    html.Div(id='data-counter', style={'display': 'flex', 'justifyContent': 'center', 'alignItems': 'center'})
                ], style={'flex': '1', 'display': 'flex', 'flexDirection': 'column', 'justifyContent': 'center', 'alignItems': 'center'})
            ], style={'display': 'flex', 'alignItems': 'center', 'justifyContent': 'space-between', 'padding': '10px', 'background': '#f7f7f7', 'borderRadius': '10px', 'marginBottom': '0'}),

            # New row for the actual vs predicted plot and larger tweets table with inputs
            html.Div([
                html.Div([
                    html.H3("Actual vs Predicted", style={'textAlign': 'center'}),
                    dcc.Graph(id='actual-vs-predicted-graph')
                ], style={'flex': '2', 'display': 'flex', 'flexDirection': 'column'}),

                html.Div([
                    html.H3("Tweets Table", style={'textAlign': 'center'}),
                    html.Div([
                        dcc.Input(id='input-date', type='date', placeholder='Select a date', style={'marginRight': '10px'}),
                        html.Button('Sample Tweets', id='sample-button', n_clicks=0)
                    ], style={'textAlign': 'center', 'margin': '10px'}),
                    html.Div(id='tweets-table-container', style={'overflowY': 'scroll', 'maxHeight': '400px'})
                ], style={'flex': '2', 'display': 'flex', 'flexDirection': 'column'})
            ], style={'display': 'flex', 'alignItems': 'start', 'justifyContent': 'space-between', 'padding': '10px', 'background': '#f7f7f7', 'borderRadius': '10px', 'marginTop': '0'}),

            dcc.Interval(id='interval-component', interval=self.interval * 1000, n_intervals=0)
        ])

        @self.app.callback(
            [Output('actual-vs-predicted-graph', 'figure'),
            Output('tweets-table-container', 'children'),
            Output('subplot-graph', 'figure'),
            Output('data-counter', 'children')],
            [Input('sample-button', 'n_clicks'),
            Input('interval-component', 'n_intervals')],
            [State('input-date', 'value')]
        )
        def update_content(n_clicks, n_intervals, input_date):
            # Check which input has triggered the callback
            changed_id = [p['prop_id'] for p in callback_context.triggered][0]

            # Updating tweets table if the sample button is clicked
            if 'sample-button' in changed_id and input_date is not None:
                selected_date = pd.to_datetime(input_date)
                matching_tweets = self.data[self.data['Date'].dt.date == selected_date.date()]

                # Sample 10 random tweets if there are enough tweets
                sampled_tweets = matching_tweets.sample(10) if len(matching_tweets) >= 10 else matching_tweets

                # Generate the tweets table
                tweets_table = html.Table([
                    html.Thead(html.Tr([html.Th('Date'), html.Th('Tweet')])),
                    html.Tbody([
                        html.Tr([html.Td(tweet['Date'].strftime('%Y-%m-%d')), html.Td(tweet['Tweet'])])
                        for _, tweet in sampled_tweets.iterrows()
                    ])
                ])
            else:
                # Keeping the last sampled tweets if the interval triggers the update
                tweets_table = dash.no_update

            # Counter for the number of tweets
            row_count = len(self.data)
            counter_display = html.Div([
                html.Div([
                    html.Div(f"{row_count}", style={
                        'border': '2px solid #4CAF50',
                        'borderRadius': '50%',
                        'color': '#4CAF50',
                        'fontSize': '36px',
                        'fontWeight': 'bold',
                        'width': '100px',
                        'height': '100px',
                        'textAlign': 'center',
                        'lineHeight': '100px',
                        'margin': '0 auto'
                    })
                ], style={'display': 'flex', 'justifyContent': 'center', 'alignItems': 'center', 'height': '150px'})
            ])

            # Subplots for stock metrics
            numeric_columns = ['Low', 'High', 'Open', 'Close', 'Adj Close', 'Volume']
            graph_data = self.data[numeric_columns + ['Date']].groupby('Date').mean().reset_index()
            subplot_fig = make_subplots(rows=2, cols=3, subplot_titles=numeric_columns)
            for i, column in enumerate(numeric_columns):
                row = (i // 3) + 1
                col = (i % 3) + 1
                subplot_fig.add_trace(
                    go.Scatter(x=graph_data['Date'], y=graph_data[column], mode='lines+markers', name=column),
                    row=row, col=col
                )
            subplot_fig.update_layout(title="Stock Metrics Over Time", showlegend=False)

            concerned_columns = ['Close', 'Prediction']
            graph_data_concerned = self.data[concerned_columns + ['Date']].groupby('Date').mean().reset_index()
            
            # Actual vs Predicted Plot
            actual_vs_predicted_fig = go.Figure()
            actual_vs_predicted_fig.add_trace(
                go.Scatter(x=graph_data_concerned['Date'], y=graph_data_concerned['Close'], mode='lines+markers', name='Actual Close')
            )
            actual_vs_predicted_fig.add_trace(
                go.Scatter(x=graph_data_concerned['Date'], y=graph_data_concerned['Prediction'], mode='lines+markers', name='Predicted Close')
            )
            actual_vs_predicted_fig.update_layout(title="Actual vs Predicted Close Values Over Time", xaxis_title="Date", yaxis_title="Price")

            # Return the updated components
            return actual_vs_predicted_fig, tweets_table, subplot_fig, counter_display


        
    def _fetch_data(self):
        for message in self._consumer:
            self.update_event.wait()  
            data = message.value
            self.queue.put(data)
            
    def update_data(self):
        i = 0
        while True:
            self.update_event.set()

            # Process all items in the queue
            while not self.queue.empty():
                data = self.queue.get()

                try:
                    # Ensure data is a list of dictionaries
                    if isinstance(data, dict):
                        data = [data]

                    new_data = pd.DataFrame(data)
                    # Drop columns in new_data that are entirely NA
                    new_data = new_data.dropna(axis=1, how='all')

                    # Realign columns to match self.data
                    new_data = new_data.reindex(columns=self.data.columns)

                    self.data = pd.concat([self.data, new_data], ignore_index=True)
                    self.data['Date'] = pd.to_datetime(self.data['Date'])
                    # sort by date
                    self.data = self.data.sort_values(by=['Date'])
                except Exception as e:
                    print("Error processing data:", e)

                self.queue.task_done()

            self.update_event.clear()  
            time.sleep(self.interval)
            print("Done updating data, new length: ", len(self.data))

    def _run(self):
        thread_fetch_data = threading.Thread(target=self._fetch_data)
        thread_fetch_data.start()

        thread_update_data = threading.Thread(target=self.update_data)
        thread_update_data.start()
        
        dash_thread = threading.Thread(target=lambda: self.app.run_server(host='0.0.0.0'))
        dash_thread.start()

    def start(self):
        print("Starting PredictionConsumer for topic: ", self._prediction_topic_name)
        self._run()

