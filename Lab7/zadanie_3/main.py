from src.utils import start_spark
from src.datareader import data_reader
from src.datawriter import data_writer
from src.datatransformer import data_transformer


SOURCE_PATH = "../../../Lab_6/Customer.csv"
OUTPUT_PATH = "output"
COLUMN = "FirstName"


def main():
    spark = start_spark()
    df = data_reader(spark, SOURCE_PATH)
    df = data_transformer(df, COLUMN)
    data_writer(df, OUTPUT_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
