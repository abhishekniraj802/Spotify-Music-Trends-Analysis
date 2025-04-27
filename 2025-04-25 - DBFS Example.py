# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/adult.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "adult_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `adult_csv`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "adult_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# Load the CSV file as raw text
rdd_raw = sc.textFile("/FileStore/tables/adult.csv")

# Skip the header
header = rdd_raw.first()
rdd = rdd_raw.filter(lambda line: line != header)

# Split each line by commas
rdd_split = rdd.map(lambda line: line.split(","))

# Show first 5 records
rdd_split.take(5)

# Filter rows where income is >50K
rdd_50k = rdd_split.filter(lambda row: row[-1].strip() == ">50K")
rdd_50k.count()

# Group by education column (index 3)
education_count = rdd_split.map(lambda row: (row[3], 1)).reduceByKey(lambda a, b: a + b)
education_count.collect()


# COMMAND ----------

# Read the CSV as DataFrame
df = spark.read.option("header", "true").csv("/FileStore/tables/adult.csv")

# Print schema
df.printSchema()

# Show first 5 records
df.show(5)

# Filter for people with income >50K
df.filter(df["income"] == ">50K").count()

# Group by education and count
df.groupBy("education").count().show()


# COMMAND ----------

# MAGIC %scala
# MAGIC case class Person(age: String, workclass: String, education: String, income: String)
# MAGIC

# COMMAND ----------

# MAGIC %scala
# MAGIC val df = spark.read.option("header", "true").csv("/FileStore/tables/adult.csv")
# MAGIC import spark.implicits._
# MAGIC
# MAGIC // Convert DataFrame to Dataset
# MAGIC val ds = df.as[Person]
# MAGIC
# MAGIC // Show first 5 records
# MAGIC ds.show(5)
# MAGIC
# MAGIC // Filter for income greater than 50K
# MAGIC ds.filter(_.income == ">50K").count()
# MAGIC
# MAGIC // Group by education and count
# MAGIC ds.groupBy("education").count().show()
# MAGIC

# COMMAND ----------

rdd_split.filter(lambda row: row[-1].strip() == ">50K" and row[3].strip() == "Bachelors").count()


# COMMAND ----------

df.filter((df["income"] == ">50K") & (df["education"] == "Bachelors")).count()


# COMMAND ----------

# MAGIC %scala
# MAGIC ds.filter(p => p.income == ">50K" && p.education == "Bachelors").count()
