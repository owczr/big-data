from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col


def to_lower(string):
    return string.lower()


udf_to_lower = udf(to_lower, StringType())


def data_transformer(df, column):
    df = df.withColumn(f"{column}_lowered", udf_to_lower(col(column)))
    return df
