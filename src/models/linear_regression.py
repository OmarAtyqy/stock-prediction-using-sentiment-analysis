import os
from pyspark.ml import PipelineModel
from ..custom_pipelines.column_selector import ColumnSelector
from ..custom_pipelines.tweet_cleaner import CleanTweetTransformer


class RegressionModel:
    """
    This class is used used as wrapper for the preprocessing pipeline and the model pipeline.
    This class should be callable to run predictions on new data.
    """

    def __init__(self, preprocessing_pipeline_path, model_path, features_col='features', target_col="Close", inputCol='Tweet', outputCol='CleanTweet'):
        if not os.path.exists("saved_models/final_model"):
            raise Exception(
                "The model has not been trained yet. Please run the model_training.py script first.")

        if not os.path.exists("saved_models/preprocessing_pipeline"):
            raise Exception(
                "The preprocessing pipeline has not been trained yet. Please run the preprocessing_pipeline.py script first.")

        # Load the preprocessing pipeline
        self.preprocessing_pipeline = PipelineModel.load(
            preprocessing_pipeline_path)

        # Load the pretrained model pipeline
        self.model_pipeline = PipelineModel.load(model_path)

        # Create the tweet cleaner and column selector transformers
        self.tweet_cleaner = CleanTweetTransformer(
            inputCol=inputCol, outputCol=outputCol)
        self.column_selector = ColumnSelector(
            outputCols=[features_col, target_col])

    def __call__(self, data):
        """
        This method is used to run predictions on new data.
        """
        # Preprocess the data
        data = self.tweet_cleaner.transform(data)
        data = self.preprocessing_pipeline.transform(data)
        data = self.column_selector.transform(data)

        # Make predictions
        predictions = self.model_pipeline.transform(data)

        # Return the predictions
        return predictions
