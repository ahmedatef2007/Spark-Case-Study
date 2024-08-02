from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import subprocess

spark = SparkSession.builder.appName("BatchProcessing").config("spark.master", "local[*]").enableHiveSupport().getOrCreate()

now = datetime.now()
last_hour = now - timedelta(hours=0)  
last_hour_dir = last_hour.strftime("/user/itversity/casestudy/rawdata/%Y/%m/%d/%H")
print(last_hour_dir)

list_groups_command = f"hdfs dfs -ls {last_hour_dir}"
result_groups = subprocess.run(list_groups_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
if result_groups.returncode != 0:
    raise Exception(f"Error listing groups: {result_groups.stderr}")

group_dirs = [line.split()[-1] for line in result_groups.stdout.splitlines() if line.startswith('d')]

file_types = ["branches_SS_raw", "sales_agents_SS_raw", "sales_transactions_SS_raw"]
dataframes = {file_type: None for file_type in file_types}

sales_transactions_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("transaction_date", DateType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("customer_fname", StringType(), True),
    StructField("cusomter_lname", StringType(), True),
    StructField("cusomter_email", StringType(), True),
    StructField("sales_agent_id", IntegerType(), True),
    StructField("branch_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("offer_1", IntegerType(), True),
    StructField("offer_2", IntegerType(), True),
    StructField("offer_3", IntegerType(), True),
    StructField("offer_4", IntegerType(), True),
    StructField("offer_5", IntegerType(), True),
    StructField("units", IntegerType(), True),
    StructField("unit_price", FloatType(), True),
    StructField("is_online", BooleanType(), True),
    StructField("payment_method", StringType(), True),
    StructField("shipping_address", StringType(), True)
])

for group_dir in group_dirs:
    for file_type in file_types:
        file_path = f"{group_dir}/{file_type}_*.csv"
        list_files_command = f"hdfs dfs -ls {file_path}"
        result_files = subprocess.run(list_files_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        if result_files.returncode != 0:
            raise Exception(f"Error listing files: {result_files.stderr}")

        files = [line.split()[-1] for line in result_files.stdout.splitlines() if line.endswith('.csv')]

        for file in files:
            df = spark.read.csv(file, header=True, inferSchema=True)
            
            if file_type == "sales_transactions_SS_raw":
                df = df.select(
                    "transaction_id", "transaction_date", "customer_id", 
                    "customer_fname", "cusomter_lname", "cusomter_email", 
                    "sales_agent_id", "branch_id", "product_id", "product_name", 
                    "product_category", "offer_1", "offer_2", "offer_3", 
                    "offer_4", "offer_5", "units", "unit_price", "is_online", 
                    "payment_method", "shipping_address"
                )
            
            if dataframes[file_type] is None:
                dataframes[file_type] = df
            else:
                dataframes[file_type] = dataframes[file_type].union(df)


branches = dataframes["branches_SS_raw"]
sales_agent = dataframes["sales_agents_SS_raw"]
sales_transactions = dataframes["sales_transactions_SS_raw"]

sales_transactions = sales_transactions.withColumnRenamed("cusomter_lname", "customer_lname") \
                                       .withColumnRenamed("cusomter_email", "customer_email")


regex_pattern = r"(\.com).*"
sales_transactions = sales_transactions.withColumn(
    "customer_email",
    regexp_replace(trim(col("customer_email")), regex_pattern, ".com")
)


sales_transactions = sales_transactions.fillna({"offer_1": False, "offer_2": False, "offer_3": False, "offer_4": False, "offer_5": False})


split_cols = split(col("shipping_address"), "/")
sales_transactions = sales_transactions.withColumn("address", split_cols.getItem(0)) \
                                       .withColumn("city", split_cols.getItem(1)) \
                                       .withColumn("state", split_cols.getItem(2)) \
                                       .withColumn("postal_code", split_cols.getItem(3))

for column in sales_transactions.columns:
    sales_transactions = sales_transactions.withColumn(column, trim(col(column)))

sales_transactions = sales_transactions.dropDuplicates()


sales_transactions_sdf = sales_transactions

sales_transactions_sdf = sales_transactions.withColumn(
    "discount_percentage",
    coalesce(
        when(col("offer_1") == True, lit(5)),
        when(col("offer_2") == True, lit(10)),
        when(col("offer_3") == True, lit(15)),
        when(col("offer_4") == True, lit(20)),
        when(col("offer_5") == True, lit(25)),
        lit(0)  # No discount if no offer applied
    )
)


sales_transactions_sdf = sales_transactions_sdf.withColumn(
    "total_paid_price",
    (col("units") * col("unit_price")) * (1 - col("discount_percentage") / 100)
)

most_selling_products = sales_transactions_sdf.groupBy("product_name").agg(sum("units").alias("total_units_sold")).orderBy(desc("total_units_sold"))


database_name = "iti_dwh"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS sales_transactions_fact (
        transaction_id STRING,
        customer_id STRING,
        sales_agent_id STRING,
        branch_id STRING,
        product_id STRING,
        units INT,
        unit_price DOUBLE,
        discount_percentage DOUBLE,
        total_paid_price DOUBLE,
        is_online BOOLEAN
    )
    PARTITIONED BY (transaction_date STRING)
    STORED AS PARQUET
""")

sales_transactions_fact = sales_transactions_sdf.select(
    col("transaction_id"),
    col("transaction_date"),
    col("customer_id"),
    col("sales_agent_id"),
    col("branch_id"),
    col("product_id"),
    col("units").cast("int").alias("units"),
    col("unit_price").cast("double").alias("unit_price"),
    col("discount_percentage").cast("double").alias("discount_percentage"),
    col("total_paid_price").cast("double").alias("total_paid_price"),
    when(col("is_online") == "yes", True)
        .when(col("is_online") == "no", False)
        .alias("is_online")
)
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
sales_transactions_fact.write \
    .mode("append") \
    .partitionBy("transaction_date") \
    .format("hive") \
    .saveAsTable(f"{database_name}.sales_transactions_fact")


sample_data = spark.sql(f"SELECT * FROM sales_transactions_fact LIMIT 20")

customer_dim = sales_transactions_sdf.groupBy("customer_id").agg(
    first("customer_fname").alias("first_name"),
    first("customer_lname").alias("last_name"),
    first("customer_email").alias("email"),
    first("address").alias("address"),
    first("city").alias("city"),
    first("state").alias("state"),
    first("postal_code").alias("postal_code")
)


spark.sql(f"USE {database_name}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS customer_dim (
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        address STRING,
        city STRING,
        state STRING,
        postal_code STRING
    )
    STORED AS PARQUET
""")


spark.catalog.refreshTable(f"iti_dwh.customer_dim")

existing_customers_df = spark.table(f"iti_dwh.customer_dim")
new_customers_df = customer_dim.alias("new").join(
    existing_customers_df.alias("existing"),
    col("new.customer_id") == col("existing.customer_id"),
    "left_anti"  
)

new_customers_df.write \
    .mode("append") \
    .format("hive") \
    .saveAsTable(f"iti_dwh.customer_dim")

sample_data = spark.sql(f"SELECT * FROM customer_dim LIMIT 10")


product_dim = sales_transactions_sdf.select(
    col("product_id"),
    col("product_name"),
    col("product_category")
).distinct()


spark.sql(f"""
    CREATE TABLE IF NOT EXISTS product_dim (
        product_id STRING,
        product_name STRING,
        product_category STRING
    )
    STORED AS PARQUET
""")

existing_products_df = spark.table("iti_dwh.product_dim")
new_products_df = product_dim.alias("new").join(
    existing_products_df.alias("existing"),
    col("new.product_id") == col("existing.product_id"),
    "left_anti"  
)

new_products_df.write \
    .mode("append") \
    .format("hive") \
    .saveAsTable(f"iti_dwh.product_dim")

sample_data = spark.sql(f"SELECT * FROM product_dim LIMIT 10")


sales_agent_dim = sales_agent.select(
    col("sales_person_id"),
    col("name")
).distinct()


spark.sql(f"""
    CREATE TABLE IF NOT EXISTS sales_agent_dim (
        sales_person_id STRING,
        name STRING
    )
    STORED AS PARQUET
""")

existing_sales_agents_df = spark.table("iti_dwh.sales_agent_dim")
new_agents_df = sales_agent_dim.alias("new").join(
    existing_sales_agents_df.alias("existing"),
    col("new.sales_person_id") == col("existing.sales_person_id"),
    "left_anti"  
)

new_agents_df.write \
    .mode("append") \
    .format("hive") \
    .saveAsTable(f"iti_dwh.sales_agent_dim")

sample_data = spark.sql(f"SELECT * FROM sales_agent_dim LIMIT 10")


branch_dim = branches.select(
    col("branch_id"),
    col("class"),
    col("location")
).distinct()


spark.sql(f"""
    CREATE TABLE IF NOT EXISTS branch_dim (
        branch_id STRING,
        class STRING,
        location STRING
    )
    STORED AS PARQUET
""")

existing_branches_df = spark.table("iti_dwh.branch_dim")
new_branches_df = branch_dim.alias("new").join(
    existing_branches_df.alias("existing"),
    col("new.branch_id") == col("existing.branch_id"),
    "left_anti"  
)


new_branches_df.write \
    .mode("append") \
    .format("hive") \
    .saveAsTable(f"iti_dwh.branch_dim")

sample_data = spark.sql(f"SELECT * FROM branch_dim LIMIT 10")
sample_data.show()


sales_transactions_fact.createOrReplaceTempView("sales_transactions_fact")
customer_dim.createOrReplaceTempView("customer_dim")
product_dim.createOrReplaceTempView("product_dim")
sales_agent_dim.createOrReplaceTempView("sales_agent_dim")
branch_dim.createOrReplaceTempView("branch_dim")



customer_purchase_behavior = spark.sql("""
    SELECT
        concat(concat(cd.first_name," ") , cd.last_name) as Customer_Name,
        COUNT(DISTINCT stf.transaction_id) AS total_transactions,
         round(SUM(stf.total_paid_price),2) AS total_spent
    FROM
        sales_transactions_fact stf
    JOIN
        customer_dim cd ON stf.customer_id = cd.customer_id
    GROUP BY
        cd.first_name, cd.last_name
    ORDER BY
        total_spent DESC
""")

customer_purchase_behavior.show()


total_sales_by_category = spark.sql("""
    SELECT
        pd.product_category,
        SUM(stf.units) AS total_units_sold,
         round(SUM(stf.total_paid_price),2) AS total_revenue
    FROM
        sales_transactions_fact stf
    JOIN
        product_dim pd ON stf.product_id = pd.product_id
    GROUP BY
        pd.product_category
    ORDER BY
        total_revenue DESC
""")

total_sales_by_category.show()


most_selling_products = spark.sql("""
    SELECT
        pd.product_name,
        SUM(stf.units) AS total_units_sold,
        round(SUM(stf.total_paid_price) ,2) AS total_revenue
    FROM
        sales_transactions_fact stf
    JOIN
        product_dim pd ON stf.product_id = pd.product_id
    GROUP BY
        pd.product_name
    ORDER BY
        total_units_sold DESC
""")

most_selling_products.show()


spark.stop()



