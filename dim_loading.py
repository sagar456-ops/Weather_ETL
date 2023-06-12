# Databricks notebook source
# Delete the existing schema if it exists
spark.sql("DROP schema IF EXISTS dim CASCADE")

# Create a new schema
spark.sql("CREATE schema dim")

# COMMAND ----------

# DBTITLE 1,creating dim date table
from pyspark.sql.functions import *
from pyspark.sql.types import BooleanType
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import hour, date_format
from delta.tables import DeltaTable



def create_date_dimension(start_date, end_date):
    # Create a list of dates for the date dimension
    date_list = spark.sql(f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) AS dates").collect()[0].dates

    # Create a DataFrame from the date list
    date_df = spark.createDataFrame([(d,) for d in date_list], ["date"])

    # Add columns for different attributes of the date
    date_df = date_df.withColumn("date_key_id", date_format("date", "yyyyMMdd"))
    date_df = date_df.withColumn("year", year("date"))
    date_df = date_df.withColumn("month", month("date"))
    date_df = date_df.withColumn("day", dayofmonth("date"))
    date_df = date_df.withColumn("quarter", quarter("date"))
    date_df = date_df.withColumn("day_of_week", date_format("date", "EEEE"))
    date_df = date_df.withColumn("day_of_week_number", expr("try_cast(date_format(date, 'E') as INT)"))
    date_df = date_df.withColumn("is_weekend", date_format("date", "E").isin(["Sat", "Sun"]).cast(BooleanType()))

    # Create the date dimension table
    date_df.createOrReplaceTempView("date_dimension")

    # Execute SQL to create the table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dim.dim_date (
            date_key_id STRING,
            date DATE,
            year INT,
            month INT,
            day INT,
            quarter INT,
            day_of_week STRING,
            day_of_week_number INT,
            is_weekend BOOLEAN
        )
        USING DELTA
    """)

    # Insert data into the date dimension table
    spark.sql("""
        INSERT INTO dim.dim_date
        SELECT
            date_key_id,
            date,
            year,
            month,
            day,
            quarter,
            day_of_week,
            day_of_week_number,
            is_weekend
        FROM
            date_dimension -- selecting from view 
    """)




# COMMAND ----------

# DBTITLE 1,creating dim time table
def create_time_dimension(start_time, end_time, interval):
    # Create a list of time intervals for the time dimension
    time_df = spark.sql(f"SELECT sequence(to_timestamp('{start_time}', 'HH:mm:ss'), to_timestamp('{end_time}', 'HH:mm:ss'), interval '{interval}') AS times").collect()[0].times

    # Create a DataFrame from the time list
    time_df = spark.createDataFrame([(t,) for t in time_df], ["datetime"])

    # Extract the time portion from the datetime column
    time_df = time_df.withColumn("time", date_format("datetime", "HH:mm:ss"))

    # Add a column for the time_id
    time_df = time_df.withColumn("time_id", row_number().over(Window.orderBy("datetime")).cast(IntegerType()))

    # Extract the hour, minute, and second from the time column
    time_df = time_df.withColumn("hour", hour("datetime"))

    # Create the time dimension table
    time_df.createOrReplaceTempView("time_dimension")

    # Execute SQL to create the table
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dim.dim_time (
            time_id INT,
            time STRING,
            hour INT
        )
        USING DELTA
    """)

    # Insert data into the time dimension table
    spark.sql("""
        INSERT INTO dim.dim_time
        SELECT
            time_id,
            time,
            hour
        FROM
            time_dimension
    """)




# COMMAND ----------

# DBTITLE 1,creating dim location table
# Create a Delta table
spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_city(city_id int,city_name string,country STRING,lat float,lon FLOAT)
""")

# COMMAND ----------


def insert_dim_city_table():
    selected_data = spark.sql("""
        SELECT DISTINCT(city_id), city_name, lat, lon, country
        FROM default.weather_silver
    """)

    # Insert the selected data into the dim_city table
    selected_data.write.format("delta").mode("append").saveAsTable("dim.dim_city")



