from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import os
import subprocess

# Create SparkSession
spark = SparkSession.builder \
    .appName("DataCompactionJob") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Define the HDFS path and current date/hour
base_path = "hdfs:///user/itversity/casestudy/kafkastream"
current_datetime = datetime.now()
last_hour_datetime = current_datetime - timedelta(hours=1)
year = last_hour_datetime.year
month = f"{last_hour_datetime.month:02d}"
day = f"{last_hour_datetime.day:02d}"
hour = f"{last_hour_datetime.hour}"

# Define the input and output path
input_path = os.path.join(base_path, f"event_date={year}-{month}-{day}/event_hour={hour}")
output_path = os.path.join(base_path, f"event_date={year}-{month}-{day}/event_hour={hour}_compacted")

# Function to check if the input path exists in HDFS
def hdfs_path_exists(path):
    try:
        subprocess.run(["hdfs", "dfs", "-test", "-e", path], check=True)
        return True
    except subprocess.CalledProcessError:
        return False

# Check if the input path exists
if hdfs_path_exists(input_path):
# Read the data from the specified path
    df = spark.read.parquet(input_path)

    # Repartition the data to control the number of output files
    # Adjust the number of partitions based on your needs
    num_partitions = 1  # You can change this number to control the number of output files
    compacted_df = df.repartition(num_partitions)

    # Write the data back to the same folder (or a different folder)
    compacted_df.write.mode("overwrite").parquet(output_path)

    # Optionally, you can delete the old data and rename the new folder to the original folder name
    # This part requires HDFS commands, which can be executed using the subprocess module in Python

    # Delete the old folder
    delete_command = f"hdfs dfs -rm -r {input_path}"
    subprocess.run(delete_command, shell=True, check=True)

    # Rename the compacted folder to the original folder name
    rename_command = f"hdfs dfs -mv {output_path} {input_path}"
    subprocess.run(rename_command, shell=True, check=True)
else:
    print("erroorrr")

spark.stop()
