from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, when, lit, coalesce, regexp_replace, trim, split
from pyspark.sql.types import *
from pyspark.sql.functions import regexp_replace, expr, col, trim
from datetime import datetime, timedelta
import subprocess

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("B2BTeamDailyDump") \
    .enableHiveSupport() \
    .config("spark.master", "local[*]") \
    .getOrCreate()

now = datetime.now()
simulated_date = now - timedelta(days=905)
formatted_date = simulated_date.strftime('%Y-%-m-%-d') 
formatted_date

spark.sql("SHOW DATABASES").show()

query = f"""
SELECT 
    a.name AS sales_agent_name,
    p.product_name AS product_name,
    SUM(t.units) AS total_sold_units
FROM 
    sales_transactions_fact t
JOIN 
    sales_agent_dim a
ON 
    t.sales_agent_id = a.sales_person_id
JOIN 
    product_dim p
ON 
    t.product_id = p.product_id
WHERE 
    date_format(t.transaction_date, 'yyyy-M-d') = '{formatted_date}'
GROUP BY 
    a.name,
    p.product_name
"""
spark.sql(f"USE iti_dwh")
fact_result = spark.sql(query)
fact_result

import pandas as pd
pdf = fact_result.toPandas()
df_sorted = pdf.sort_values(by='total_sold_units', ascending=False)

# Save the DataFrame as a single CSV file
df_sorted.to_csv(f"/home/itversity/itversity-material/caseStudy/sales_data_{formatted_date}.csv", index=False)

spark.stop()

