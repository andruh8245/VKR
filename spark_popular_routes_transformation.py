from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('SalesAnalyses') \
    .getOrCreate()

jdbc_url = "jdbc:postgresql://dev1c-5:5432/booking>"
properties = {
    "user": "uak",
    "password": "uak",
    "driver": "org.postgresql.Driver"
}

sales_df = spark.read.jdbc(
    url=jdbc_url, table='bookings_details', properties=properties)

df_agg = sales_df.groupBy("route").count().orderBy("count", ascending=False)

df_agg.write.csv("/tmp/popular_routes.csv", header=True)

spark.stop()
