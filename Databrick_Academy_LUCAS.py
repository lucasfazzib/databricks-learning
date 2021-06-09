# Databricks notebook source
username = "lucasfazzi"
dbutils.widgets.text("username", username)
spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_{username}")
spark.sql(f"USE dbacademy_{username}")
health_tracker = f"/dbacademy/{username}/DLRS/healthtracker/"

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 8)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_1.json
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_2.json
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_2_late.json
# MAGIC wget https://hadoop-and-big-data.s3-us-west-2.amazonaws.com/fitness-tracker/health_tracker_data_2020_3.json

# COMMAND ----------

# MAGIC %sh ls

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_1.json", 
              health_tracker + "raw/health_tracker_data_2020_1.json")
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_2.json", 
              health_tracker + "raw/health_tracker_data_2020_2.json")
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_2_late.json", 
              health_tracker + "raw/health_tracker_data_2020_2_late.json")
dbutils.fs.mv("file:/databricks/driver/health_tracker_data_2020_3.json", 
              health_tracker + "raw/health_tracker_data_2020_3.json")

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_1.json"
 
health_tracker_data_2020_1_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

display(health_tracker_data_2020_1_df)

# COMMAND ----------

dbutils.fs.rm(health_tracker + "processed", recurse=True)

# COMMAND ----------

dbutils.fs.ls(health_tracker + "processed")

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime
 
def process_health_tracker_data(dataframe):
  return (
    dataframe
    .withColumn("time", from_unixtime("time"))
    .withColumnRenamed("device_id", "p_device_id")
    .withColumn("time", col("time").cast("timestamp"))
    .withColumn("dte", col("time").cast("date"))
    .withColumn("p_device_id", col("p_device_id").cast("integer"))
    .select("dte", "time", "heartrate", "name", "p_device_id")
    )
  
processedDF = process_health_tracker_data(health_tracker_data_2020_1_df)

# COMMAND ----------

(processedDF.write
 .mode("overwrite")
 .format("parquet")
 .partitionBy("p_device_id")
 .save(health_tracker + "processed"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS health_tracker_processed;
# MAGIC 
# MAGIC CREATE TABLE health_tracker_processed                        
# MAGIC USING PARQUET                
# MAGIC LOCATION "/dbacademy/$username/DLRS/healthtracker/processed"

# COMMAND ----------

health_tracker_processed = spark.read.table("health_tracker_processed")
health_tracker_processed.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MSCK REPAIR TABLE health_tracker_processed

# COMMAND ----------

health_tracker_processed.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE DETAIL health_tracker_processed

# COMMAND ----------

from delta.tables import DeltaTable

parquet_table = f"parquet.`{health_tracker}processed`"
partitioning_scheme = "p_device_id int"

DeltaTable.convertToDelta(spark, parquet_table, partitioning_scheme)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS health_tracker_processed;
# MAGIC 
# MAGIC CREATE TABLE health_tracker_processed
# MAGIC USING DELTA
# MAGIC LOCATION "/dbacademy/$username/DLRS/healthtracker/processed"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE DETAIL health_tracker_processed

# COMMAND ----------

health_tracker_processed = spark.read.table("health_tracker_processed")
health_tracker_processed.count()

# COMMAND ----------

dbutils.fs.rm(health_tracker + "gold/health_tracker_user_analytics",
              recurse=True)

# COMMAND ----------

from pyspark.sql.functions import col, avg, max, stddev

health_tracker_gold_user_analytics = (
  health_tracker_processed
  .groupby("p_device_id")
  .agg(avg(col("heartrate")).alias("avg_heartrate"),
       max(col("heartrate")).alias("max_heartrate"),
       stddev(col("heartrate")).alias("stddev_heartrate"))
)

# COMMAND ----------

(health_tracker_gold_user_analytics.write
 .format("delta")
 .mode("overwrite")
 .save(health_tracker + "gold/health_tracker_user_analytics"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS health_tracker_gold_user_analytics;
# MAGIC 
# MAGIC CREATE TABLE health_tracker_gold_user_analytics
# MAGIC USING DELTA
# MAGIC LOCATION "/dbacademy/$username/DLRS/healthtracker/gold/health_tracker_user_analytics"

# COMMAND ----------

display(spark.read.table("health_tracker_gold_user_analytics"))

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_2.json"
 
health_tracker_data_2020_2_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

processedDF = process_health_tracker_data(health_tracker_data_2020_2_df)

# COMMAND ----------

(processedDF.write
 .mode("append")
 .format("delta")
 .save(health_tracker + "processed"))

# COMMAND ----------

(spark.read
 .option("versionAsOf", 0)
 .format("delta")
 .load(health_tracker + "processed")
 .count())

# COMMAND ----------

health_tracker_processed.count()

# COMMAND ----------

from pyspark.sql.functions import count

display(
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .groupby("p_device_id")
  .agg(count("*"))
)

# COMMAND ----------

display(
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .where(col("p_device_id").isin([3,4]))
)

# COMMAND ----------

broken_readings = (
  health_tracker_processed
  .select(col("heartrate"), col("dte"))
  .where(col("heartrate") < 0)
  .groupby("dte")
  .agg(count("heartrate"))
  .orderBy("dte")
)
 
broken_readings.createOrReplaceTempView("broken_readings")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM broken_readings

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC SELECT SUM(`count(heartrate)`) FROM broken_readings

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, lead
 
dteWindow = Window.partitionBy("p_device_id").orderBy("dte")
 
interpolatedDF = (
  spark.read
  .table("health_tracker_processed")
  .select(col("dte"),
          col("time"),
          col("heartrate"),
          lag(col("heartrate")).over(dteWindow).alias("prev_amt"),
          lead(col("heartrate")).over(dteWindow).alias("next_amt"),
          col("name"),
          col("p_device_id"))
)

# COMMAND ----------

updatesDF = (
  interpolatedDF
  .where(col("heartrate") < 0)
  .select(col("dte"),
          col("time"),
          ((col("prev_amt") + col("next_amt"))/2).alias("heartrate"),
          col("name"),
          col("p_device_id"))
)

# COMMAND ----------

health_tracker_processed.printSchema()
updatesDF.printSchema()

# COMMAND ----------

updatesDF.count()

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_2_late.json"
 
health_tracker_data_2020_2_late_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

insertsDF = process_health_tracker_data(health_tracker_data_2020_2_late_df)

# COMMAND ----------

insertsDF.printSchema()

# COMMAND ----------

upsertsDF = updatesDF.union(insertsDF)

# COMMAND ----------

upsertsDF.printSchema()

# COMMAND ----------

processedDeltaTable = DeltaTable.forPath(spark, health_tracker + "processed")

update_match = """
  health_tracker.time = upserts.time 
  AND 
  health_tracker.p_device_id = upserts.p_device_id
"""

update = { "heartrate" : "upserts.heartrate" }

insert = {
  "p_device_id" : "upserts.p_device_id",
  "heartrate" : "upserts.heartrate",
  "name" : "upserts.name",
  "time" : "upserts.time",
  "dte" : "upserts.dte"
}

(processedDeltaTable.alias("health_tracker")
 .merge(upsertsDF.alias("upserts"), update_match)
 .whenMatchedUpdate(set=update)
 .whenNotMatchedInsert(values=insert)
 .execute())

# COMMAND ----------

(spark.read
 .option("versionAsOf", 1)
 .format("delta")
 .load(health_tracker + "processed")
 .count())

# COMMAND ----------

health_tracker_processed.count()

# COMMAND ----------

display(processedDeltaTable.history())

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT SUM(`count(heartrate)`) FROM broken_readings

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC SELECT SUM(`count(heartrate)`) FROM broken_readings WHERE dte < '2020-02-25'

# COMMAND ----------

updatesDF.count()

# COMMAND ----------

upsertsDF = updatesDF

(processedDeltaTable.alias("health_tracker")
 .merge(upsertsDF.alias("upserts"), update_match)
 .whenMatchedUpdate(set=update)
 .whenNotMatchedInsert(values=insert)
 .execute())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT SUM(`count(heartrate)`) FROM broken_readings

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_3.json"

health_tracker_data_2020_3_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

def process_health_tracker_data(dataframe):
  return (
    dataframe
    .withColumn("time", from_unixtime("time"))
    .withColumnRenamed("device_id", "p_device_id")
    .withColumn("time", col("time").cast("timestamp"))
    .withColumn("dte", col("time").cast("date"))
    .withColumn("p_device_id", col("p_device_id").cast("integer"))
    .select("dte", "time", "device_type", "heartrate", "name", "p_device_id")
    )
  
processedDF = process_health_tracker_data(health_tracker_data_2020_3_df)

# COMMAND ----------

(processedDF.write
 .mode("append")
 .format("delta")
 .save(health_tracker + "processed"))

# COMMAND ----------

(processedDF.write
 .mode("append")
 .option("mergeSchema", True)
 .format("delta")
 .save(health_tracker + "processed"))

# COMMAND ----------

health_tracker_processed.count()

# COMMAND ----------

processedDeltaTable.delete("p_device_id = 4")

# COMMAND ----------

from pyspark.sql.functions import lit

upsertsDF = (
  spark.read
  .option("versionAsOf", 4)
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
  .select("dte", "time", "device_type", "heartrate", lit(None).alias("name"), "p_device_id")
)

# COMMAND ----------

processedDeltaTable = DeltaTable.forPath(spark, health_tracker + "processed")

insert = {
  "dte" : "upserts.dte",
  "time" : "upserts.time",
  "device_type" : "upserts.device_type",
  "heartrate" : "upserts.heartrate",
  "name" : "upserts.name",
  "p_device_id" : "upserts.p_device_id"
}

(processedDeltaTable.alias("health_tracker")
 .merge(upsertsDF.alias("upserts"), update_match)
 .whenMatchedUpdate(set=update)
 .whenNotMatchedInsert(values=insert)
 .execute())

# COMMAND ----------

health_tracker_processed.count()

# COMMAND ----------

display(health_tracker_processed.where("p_device_id = 4"))

# COMMAND ----------

display(
  spark.read
  .option("versionAsOf", 4)
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
)

# COMMAND ----------

processedDeltaTable.vacuum(0)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

processedDeltaTable.vacuum(0)

# COMMAND ----------

display(
  spark.read
  .option("versionAsOf", 4)
  .format("delta")
  .load(health_tracker + "processed")
  .where("p_device_id = 4")
)
