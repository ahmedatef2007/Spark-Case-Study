from pyspark.sql import SparkSession
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("hiveRepair") \
    .enableHiveSupport() \
    .config("spark.master", "local[*]") \
    .getOrCreate()
spark.sql("MSCK REPAIR TABLE kafka").show()
spark.stop()