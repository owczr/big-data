from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


def start_spark():
    sc = SparkContext('local')
    spark = SparkSession(sc)
    return spark
