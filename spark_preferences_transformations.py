from pyspark.sql import SparkSession


def extract_preferences(input_path, output_path):
    spark = SparkSession.builder \
        .appName("Session") \
        .getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    df_prefs = df.select("sales_channel", "preferred_seat", "in_flight_meals")\
        .groupBy("sales_channel")\
        .agg({
            "preferred_seat": "sum",
            "in_flight_meals": "sum"
        })

    df_prefs.write.csv(output_path, header=True)
