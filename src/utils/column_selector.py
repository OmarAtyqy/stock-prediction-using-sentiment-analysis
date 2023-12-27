from pyspark.ml import Transformer
from pyspark.sql.functions import col


class ColumnSelector(Transformer):
    """
    A custom Transformer selects a subset of columns from a DataFrame.
    """

    def __init__(self, outputCols):
        super(ColumnSelector, self).__init__()
        self.outputCols = outputCols

    def _transform(self, df):
        return df.select(*self.outputCols)
