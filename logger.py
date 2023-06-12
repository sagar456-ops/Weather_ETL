# Databricks notebook source
import requests
import uuid
from datetime import datetime
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, ArrayType
from pyspark.sql.functions import from_unixtime
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,creating log table
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS weather_log_table_new (
# MAGIC   id STRING,
# MAGIC   load_type STRING,
# MAGIC   table_name STRING,
# MAGIC   process_start_time TIMESTAMP,
# MAGIC   process_end_time TIMESTAMP,
# MAGIC   status STRING,
# MAGIC   comments STRING,
# MAGIC   start_date_time TIMESTAMP,
# MAGIC   end_date_time TIMESTAMP,
# MAGIC   created_on TIMESTAMP,
# MAGIC   created_by STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# DBTITLE 1,decorator to update log 
def keep_log(func):
    def wrapper(*args, **kwargs):
        id = str(uuid.uuid4())
        load_type = args[0]
        table_name = args[1]
        process_start_dt = datetime.now()
        name = 'Sagar Rimal'

        # Populating log table
        query = f"insert into weather_log_table_new (id, load_type, table_name, process_start_time, status, created_on, created_by) \
            values ('{id}', '{load_type}', '{table_name}', '{process_start_dt}', 'EXTRACTING', '{process_start_dt}', '{name}')"
        spark.sql(query)

        df, start, end = func(*args[2:], **kwargs)

        udf_id = udf(lambda: id)
        udf_created_on = udf(lambda: process_start_dt, TimestampType())
        udf_created_by = udf(lambda: name)

        df = df.withColumn('load_run_id', udf_id())
        df = df.withColumn('created_on', udf_created_on())
        df = df.withColumn('created_by', udf_created_by())

        df.write.format('delta').mode('append').saveAsTable(table_name)

        process_end_dt = datetime.now()
        query = f"update weather_log_table_new \
                    set process_end_time = '{process_end_dt}', status='COMPLETED', start_date_time='{start}', end_date_time='{end}' \
                    where id='{id}'"
        spark.sql(query)

        return df

    return wrapper

