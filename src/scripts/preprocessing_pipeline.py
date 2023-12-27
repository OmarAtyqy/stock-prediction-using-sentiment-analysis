# The purpose of this script is to create a pipeline that will be used to preprocess the data
# The incoming data and the data used to train the model should contain the following columns:
# - Date: The date of the tweet
# - Tweet: The raw tweet itself
# - Stock Name: The symbol of the stock that the tweet is about
# - Open: The opening price of the stock on that day
# - Close: The closing price of the stock on that day
# - High: The highest price of the stock on that day
# - Low: The lowest price of the stock on that day
# - Volume: The volume of the stock on that day
# - Adj Close: The adjusted closing price of the stock on that day
# The pipeline will take in the data and do the following:
# - Drop the Date and Stock Name columns
# - Clean, tokenize and embed the tweets using Word2Vec
# - Scale (normalize and standardize) the numerical data
# - Assemble the features (embedded tweets and scaled numerical data) into a single vector
# - return the features and the target column (Close)


from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, StructType, StructField
from pyspark.ml.feature import MinMaxScaler, Tokenizer, Word2Vec, VectorAssembler
from pyspark.ml import Pipeline

from ..utils.tweet_cleaner import CleanTweetTransformer
from ..utils.column_selector import ColumnSelector

# define the target column
target_col = 'Close'

# Define the input features
input_cols = ['Open', 'High', 'Low', 'Volume', 'Adj Close']

# Define the schema for your data
schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Tweet", StringType(), True),
    StructField("Stock Name", StringType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Adj Close", DoubleType(), True),
    StructField("Volume", DoubleType(), True),
])

# Define the paths for saving
pipeline_path = "models/preprocessing_pipeline"
final_data_path = "data/final_data"

# ================================== Reading the data ============================================
print("Reading the data...")

# Create Spark session
spark = SparkSession.builder \
    .appName("preprocessing-pipeline") \
    .getOrCreate()

# read the data
df = spark.read.csv("data/joined_data.csv", header=True, schema=schema)
print("Number of rows: {}".format(df.count()))

# Drop 'Date' and 'Stock Name' columns and remove rows with null values
df = df.drop('Date', 'Stock Name')
df = df.dropna()
print("Number of rows after dropping null values: {}".format(df.count()))

# ================================== Creating the pipline ============================================
print("Creating the pipeline...")

# Initialize the custom Cleaner Transformer
cleaner = CleanTweetTransformer(inputCol='Tweet', outputCol='CleanTweet')

# Tokenize the cleaned tweets
tokenizer = Tokenizer(inputCol='CleanTweet', outputCol='TweetTokens')

# Embed the tweets using Word2Vec
word2Vec = Word2Vec(vectorSize=100, minCount=0,
                    inputCol='TweetTokens', outputCol='EmbeddedTweet')

# Scale the numerical data
assembler_1 = VectorAssembler(
    inputCols=input_cols, outputCol='AssembledFeatures1')
scaler = MinMaxScaler(inputCol="AssembledFeatures1",
                      outputCol="ScaledFeatures")

# Assemble the features into a single vector
assembler_2 = VectorAssembler(
    inputCols=['EmbeddedTweet', 'ScaledFeatures'], outputCol='features')

# create the column selector
column_selector = ColumnSelector(outputCols=['features', target_col])

# Create the pipeline
pipeline = Pipeline(stages=[cleaner, tokenizer, word2Vec,
                            assembler_1, scaler, assembler_2, column_selector])

# ================================== Running the pipeline ============================================
print("Running the pipeline...")

# Fit the pipeline to the data
pipeline_model = pipeline.fit(df)

# Transform the data
final_df = pipeline_model.transform(df)

# print the schema of the transformed data
print("Schema of the transformed data:")
final_df.printSchema()

# ================================== Final checks ============================================
# Check the number of rows
print("Number of rows: {}".format(final_df.count()))

# Check the number of columns
print("Number of columns: {}".format(len(final_df.columns)))

# Check the number of features
print("Number of features: {}".format(
    len(final_df.select('features').take(1)[0][0])))

# Check the number of rows with null values
print("Number of rows with null values: {}".format(
    final_df.filter(final_df[target_col].isNull()).count()))

# Check the number of rows with NaN values
print("Number of rows with NaN values: {}".format(
    final_df.filter(final_df[target_col] == float('nan')).count()))

# ================================== Saving the data ============================================
print("Saving the data and pipeline...")

# Save the preprocessing pipeline
# remove the tweet_cleaner and column_selector stages since they can't be saved
pipeline_model.stages = pipeline_model.stages[1:-1]
pipeline_model.save(pipeline_path)

# Save the final DataFrame
final_df.write.format("parquet").save(final_data_path)

# Stop the Spark session
spark.stop()

print("Data and pipeline saved successfully.")
