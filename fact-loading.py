# Databricks notebook source
# DBTITLE 1,Creating fact schema
# Delete the existing schema if it exists
spark.sql("DROP schema IF EXISTS fact CASCADE")

# Create a new schema
spark.sql("CREATE schema fact")

# COMMAND ----------

# MAGIC %run ../My_etl_module/logger

# COMMAND ----------

# DBTITLE 1,Creating hourly_weather_fact_table
from pyspark.sql.functions import hour, avg, to_date, date_format, max,row_number,col
from pyspark.sql.window import Window

@keep_log
def insert_latest_hourly_weather_data(): #function to insert hourly weather data
    # Load the dimension tables
    dim_time = spark.table("dim.dim_time")
    dim_city = spark.table("dim.dim_city")
    dim_date = spark.table("dim.dim_date")

    # Load the cleaned weather_silver table
    weather_data = spark.table("default.weather_silver")

    weather_data = weather_data.withColumn("dateid", date_format(to_date(weather_data.date, "yyyy-MM-dd HH:mm:ss"), "yyyyMMdd"))
    
    # Extract the hour from the timestamp and dateid
    weather_data = weather_data.withColumn("weather_hour", hour(weather_data.date))
    

    # Perform the joins and select the desired columns
    joined_data = weather_data.join(
        dim_time,
        weather_data.weather_hour == dim_time.time_id,
        "inner"
    ).join(
        dim_city,
        weather_data.city_id == dim_city.city_id,
        "inner"
    ).join(
        dim_date,
       dim_date.date_key_id == weather_data.dateid,  
        "inner"
    ).select(
        dim_time.time_id,
        dim_date.date_key_id,
        dim_city.city_id,
        weather_data.temp,
        weather_data.wind_speed,
        weather_data.temp_min,
        weather_data.temp_max,
        weather_data.pressure,
        weather_data.humidity,
        weather_data.visibility,
        weather_data.wind_deg,
        weather_data.wind_gust,
        weather_data.clouds_all,
        weather_data.load_run_id,
        weather_data.created_on,
        weather_data.created_by,
        weather_data.date
    )

    # Get the latest "created_on" time for each city_id by time_id
    window_spec = Window.partitionBy("city_id", "time_id").orderBy(joined_data.created_on.desc())
    latest_data = joined_data.withColumn("rn", row_number().over(window_spec)).where(col("rn") == 1).drop("rn")

    

    # Insert the latest data into the hourly_weather_data Delta table
    #latest_data.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("fact.hourly_weather_data")
    min_created_date = joined_data.selectExpr("min(date)").first()[0]
    max_created_date = joined_data.selectExpr("max(date)").first()[0]

    return latest_data,min_created_date,max_created_date


# COMMAND ----------

# DBTITLE 1,Creating daily weather fact table with average value of all hour over day
from pyspark.sql.functions import max, col

@keep_log
def insert_latest_daily_weather_data():
    # Load the dimension tables
    dim_date = spark.table("dim.dim_date")
    dim_city = spark.table("dim.dim_city")

    # Load the cleaned weather_silver table
    weather_data = spark.table("default.weather_silver")

    weather_data = weather_data.withColumn("dateid", date_format(to_date(weather_data.date, "yyyy-MM-dd HH:mm:ss"), "yyyyMMdd"))

    # Calculate the average weather data for each day
    window_spec = Window.partitionBy(weather_data.dateid, weather_data.city_id)
    avg_weather_data = weather_data.withColumn("avg_temperature", avg(weather_data.temp).over(window_spec)) \
                                   .withColumn("avg_wind_speed", avg(weather_data.wind_speed).over(window_spec)) \
                                   .withColumn("avg_temp_min", avg(weather_data.temp_min).over(window_spec)) \
                                   .withColumn("avg_temp_max", avg(weather_data.temp_max).over(window_spec)) \
                                   .withColumn("avg_pressure", avg(weather_data.pressure).over(window_spec)) \
                                   .withColumn("avg_humidity", avg(weather_data.humidity).over(window_spec)) \
                                   .withColumn("avg_visibility", avg(weather_data.visibility).over(window_spec)) \
                                   .withColumn("avg_wind_deg", avg(weather_data.wind_deg).over(window_spec)) \
                                   .withColumn("avg_wind_gust", avg(weather_data.wind_gust).over(window_spec)) \
                                   .withColumn("avg_clouds_all", avg(weather_data.clouds_all).over(window_spec))

    # Perform the joins and select the desired columns
    joined_data = avg_weather_data.join(
        dim_date,
        weather_data.dateid == dim_date.date_key_id,
        "inner"
    ).join(
        dim_city,
        avg_weather_data.city_id == dim_city.city_id,
        "inner"
    ).select(
        dim_date.date_key_id,
        dim_city.city_id,
        avg_weather_data.avg_temperature,
        avg_weather_data.avg_wind_speed,
        avg_weather_data.avg_temp_min,
        avg_weather_data.avg_temp_max,
        avg_weather_data.avg_pressure,
        avg_weather_data.avg_humidity,
        avg_weather_data.avg_visibility,
        avg_weather_data.avg_wind_deg,
        avg_weather_data.avg_wind_gust,
        avg_weather_data.avg_clouds_all,
        weather_data.created_on.alias("created_date")
    )

    # Get the latest "created_on" time for each unique dateid
    latest_created_data = joined_data.groupBy("date_key_id").agg(max("created_date").alias("latest_created_date"))

    # Join with joined_data to get the rows with the latest created_on time
    latest_data = joined_data.join(latest_created_data, joined_data.date_key_id == latest_created_data.date_key_id, "inner") \
                            .where(joined_data.created_date == latest_created_data.latest_created_date) \
                            .select(joined_data["*"])

    # Select the minimum and maximum created_date values from the latest_data DataFrame
    min_created_date = latest_data.selectExpr("min(created_date)").first()[0]
    max_created_date = latest_data.selectExpr("max(created_date)").first()[0]

    return latest_data, min_created_date, max_created_date


# COMMAND ----------


