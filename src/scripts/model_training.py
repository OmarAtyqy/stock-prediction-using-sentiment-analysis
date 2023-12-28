# The purpose of this script is to train the model and save it
# The best model found in the model selection script is Linear Regression
# As it requires no hyperparameter tuning, it will be trained and saved in this script

from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession


# data path
final_data_path = "data/final_data"

# Specify the target column and features column
target_col = "Close"
features_col = "features"

# ================================== Creating the Spark session ============================================
print("Creating the Spark session...")

# Create Spark session
spark = SparkSession.builder \
    .appName("model-training") \
    .getOrCreate()

# ================================== Reading the data ============================================
print("Reading the data...")

# Read the data
final_data = spark.read.parquet(final_data_path)

# ================================== Training the model ============================================
print("Training the model...")

# Create the model
lr = LinearRegression(featuresCol=features_col, labelCol=target_col)

# Create the pipeline
pipeline = Pipeline(stages=[lr])

# Fit the model
model = pipeline.fit(final_data)

# ================================== Saving the model ============================================
print("Saving the model...")

# Save the model
model.save("models/final_model")

# close the Spark session
spark.stop()
