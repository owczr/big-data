def data_reader(spark, filepath, filetype="csv"):
    df = spark.read.format(filetype) \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load(filepath)

    return df
