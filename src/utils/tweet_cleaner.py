from pyspark.ml.pipeline import Transformer
from pyspark.sql.functions import col, lower, regexp_replace


class CleanTweetTransformer(Transformer):
    """
    A custom Transformer that applies a cleaning function to tweet text using only built-in Spark methods.
    """

    def __init__(self, inputCol, outputCol):
        super(CleanTweetTransformer, self).__init__()
        self.inputCol = inputCol
        self.outputCol = outputCol

    def _transform(self, df):
        # Convert text to lowercase
        df = df.withColumn(self.outputCol, lower(col(self.inputCol)))

        # Replace emojis with text representation (rudimentary version)
        df = df.withColumn(self.outputCol, regexp_replace(
            col(self.outputCol), ":[a-z_]+:", ""))

        # Remove URLs
        df = df.withColumn(self.outputCol, regexp_replace(
            col(self.outputCol), "http\\S+|www\\S+|https\\S+", ""))

        # Remove user @ references and hashtags
        df = df.withColumn(self.outputCol, regexp_replace(
            col(self.outputCol), "@\\w+|#\\w+", ""))

        # Remove punctuations and special characters (except $ and %)
        df = df.withColumn(self.outputCol, regexp_replace(
            col(self.outputCol), "[^\\w\\s$%]", ""))

        # if the tweet is empty after cleaning, remove the row
        df = df.filter(df[self.outputCol] != '')

        return df
