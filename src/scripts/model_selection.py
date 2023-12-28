# The purpose of this script is to compare between different models and select the best one
# The models tested are:
# - Linear Regression
# - Decision Tree Regression
# - Random Forest Regression
# - Gradient Boosted Tree Regression
# Each model will be trained and cross-validated using the same data, after which the best model will be selected
# The fine-tuning will be done in another script

from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql import SparkSession
import numpy as np
import pandas as pd


# data path
final_data_path = "data/final_data"

# Specify the target column and features column
target_col = "Close"
features_col = "features"

# ================================== Creating the Spark session ============================================
print("Creating the Spark session...")

# Create Spark session
spark = SparkSession.builder \
    .appName("model-selection") \
    .getOrCreate()

# ================================== Reading the data ============================================
print("Reading the data...")

# Read the data
final_df = spark.read.parquet(final_data_path)

# Split the data into training and testing sets
train_data, test_data = final_df.randomSplit([0.8, 0.2], seed=42)

# ================================== Creating the models ============================================
print("Creating the models...")

models = {
    'Linear Regression': LinearRegression(labelCol=target_col, featuresCol=features_col),
    'Decision Tree Regression': DecisionTreeRegressor(labelCol=target_col, featuresCol=features_col),
    'Random Forest Regression': RandomForestRegressor(labelCol=target_col, featuresCol=features_col),
    'Gradient Boosted Tree Regression': GBTRegressor(labelCol=target_col, featuresCol=features_col)
}

# ================================== Cross-validating the models ============================================
print("Cross-validating the models...")

# Create a DataFrame to store the results
results_df = pd.DataFrame(columns=[
                          'Model', 'Average RMSE', 'Standard Deviation', 'Maximum RMSE', 'Minimum RMSE', 'Average R²'])

for model_name, model in models.items():
    print(f"Training and evaluating {model_name}...")

    # Define a grid of parameters to search over
    paramGrid = ParamGridBuilder().addGrid(model.maxDepth, [5, 10]).build(
    ) if model_name != 'Linear Regression' else ParamGridBuilder().build()

    # Set up the CrossValidator with RMSE evaluator
    crossval_rmse = CrossValidator(
        estimator=model,
        estimatorParamMaps=paramGrid,
        evaluator=RegressionEvaluator(
            labelCol=target_col, predictionCol="prediction", metricName="rmse"),
        numFolds=5)

    # Set up a separate evaluator for R² score
    r2_evaluator = RegressionEvaluator(
        labelCol=target_col, predictionCol="prediction", metricName="r2")

    # Fit the model and perform cross-validation
    cvModel = crossval_rmse.fit(train_data)

    # Collect RMSE from cross-validation
    metric_rmse = np.array(cvModel.avgMetrics)

    # Calculate RMSE statistics
    avg_rmse = np.mean(metric_rmse)
    std_deviation = np.std(metric_rmse)
    max_rmse = np.max(metric_rmse)
    min_rmse = np.min(metric_rmse)

    # Use the best model from the cross-validation to calculate the average R² score on the training data
    avg_r2 = r2_evaluator.evaluate(cvModel.bestModel.transform(train_data))

    # Print out the statistics
    print(f"{model_name} statistics:")
    print(f"Average RMSE: {avg_rmse}")
    print(f"Standard Deviation: {std_deviation}")
    print(f"Maximum RMSE: {max_rmse}")
    print(f"Minimum RMSE: {min_rmse}")
    print(f"Average R²: {avg_r2}\n")

    # Add the results to the DataFrame
    results_df = pd.concat([results_df, pd.DataFrame([[model_name, avg_rmse, std_deviation, max_rmse, min_rmse, avg_r2]], columns=[
                            'Model', 'Average RMSE', 'Standard Deviation', 'Maximum RMSE', 'Minimum RMSE', 'Average R²'])])

# ================================== Saving the results ============================================
print("Saving the results...")

# Save the results
results_df.to_csv('data/model_selection_results.csv', index=False)

# Stop the Spark session
spark.stop()
