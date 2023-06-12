# Databricks notebook source
# MAGIC %sql
# MAGIC alter table fact.hourly_weather_data add column Forcasted string;
# MAGIC -- drop table fact.hourly_weather_data;
# MAGIC --truncate table fact.hourly_weather_data;

# COMMAND ----------

from pyspark.sql.functions import hour, avg, to_date, date_format, max,row_number,col,when,lit
from pyspark.sql.window import Window

#def forcast():
# Load the dimension tables
dim_date = spark.table("dim.dim_date")
dim_city = spark.table("dim.dim_city")

# Load the cleaned weather_silver table
weather_data = spark.table("fact.hourly_weather_data")

weather_data = weather_data.withColumn("dateid", date_format(to_date(weather_data.date, "yyyy-MM-dd HH:mm:ss"), "yyyyMMdd"))

# Calculate the average weather data for each day
window_spec = Window.partitionBy(weather_data.dateid, weather_data.city_id)
avg_weather_data = weather_data.withColumn("tempe", avg(weather_data.temp).over(window_spec)) \
                        .withColumn("wind_speed", avg(weather_data.wind_speed).over(window_spec)) \
                        .withColumn("temp_min", avg(weather_data.temp_min).over(window_spec)) \
                        .withColumn("temp_max", avg(weather_data.temp_max).over(window_spec)) \
                        .withColumn("pressure", avg(weather_data.pressure).over(window_spec)) \
                        .withColumn("humidity", avg(weather_data.humidity).over(window_spec)) \
                        .withColumn("visibility", avg(weather_data.visibility).over(window_spec)) \
                        .withColumn("wind_deg", avg(weather_data.wind_deg).over(window_spec)) \
                        .withColumn("wind_gust", avg(weather_data.wind_gust).over(window_spec)) \
                        .withColumn("clouds_all", avg(weather_data.clouds_all).over(window_spec))

# Cast the avg_wind_speed column to integer

avg_weather_data = avg_weather_data.withColumn("wind_speed", col("wind_speed").cast("integer"))
avg_weather_data = avg_weather_data.withColumn("temp_min", col("temp_min").cast("float"))
avg_weather_data = avg_weather_data.withColumn("temp_max", col("temp_max").cast("float"))
avg_weather_data = avg_weather_data.withColumn("pressure", col("pressure").cast("integer"))
avg_weather_data = avg_weather_data.withColumn("humidity", col("humidity").cast("integer"))
avg_weather_data = avg_weather_data.withColumn("visibility", col("visibility").cast("integer"))
avg_weather_data = avg_weather_data.withColumn("wind_deg", col("wind_deg").cast("integer"))
avg_weather_data = avg_weather_data.withColumn("wind_gust", col("wind_gust").cast("float"))
avg_weather_data = avg_weather_data.withColumn("clouds_all", col("clouds_all").cast("float"))

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
weather_data.time_id,
dim_date.date_key_id,
dim_city.city_id,
avg_weather_data.temp,
avg_weather_data.wind_speed,
avg_weather_data.temp_min,
avg_weather_data.temp_max,
avg_weather_data.pressure,
avg_weather_data.humidity,
avg_weather_data.visibility,
avg_weather_data.wind_deg,
avg_weather_data.wind_gust,
avg_weather_data.clouds_all,
avg_weather_data.load_run_id,
avg_weather_data.created_on,
avg_weather_data.created_by,
avg_weather_data.date,
weather_data.created_on.alias("created_date"),
weather_data.Forcasted
)

# Get the latest "created_on" time for each unique dateid
latest_created_data = joined_data.groupBy("date_key_id").agg(max("created_date").alias("latest_created_date"))

# Join with joined_data to get the rows with the latest created_on time
latest_data = joined_data.join(latest_created_data, joined_data.date_key_id == latest_created_data.date_key_id, "inner") \
                    .where(joined_data.created_date == latest_created_data.latest_created_date) \
                    .select(joined_data["*"])

# Calculate the forecasted data for the next 4 to 5 hours
window_spec = Window.partitionBy("city_id").orderBy(col("time_id").desc())
forecast_data = latest_data.withColumn("forecast_time_id", col("time_id") + lit(1))
forecast_data = forecast_data.select(
forecast_data.forecast_time_id.alias("time_id"),
forecast_data.date_key_id,
forecast_data.city_id,
forecast_data.temp,
forecast_data.wind_speed,
forecast_data.temp_min,
forecast_data.temp_max,
forecast_data.pressure,
forecast_data.humidity,
forecast_data.visibility,
forecast_data.wind_deg,
forecast_data.wind_gust,
forecast_data.clouds_all,
avg_weather_data.load_run_id,
avg_weather_data.created_on,
avg_weather_data.created_by,
avg_weather_data.date,
forecast_data.created_date,
lit("True").alias("Forcasted")
).withColumn("row_num", row_number().over(window_spec)).where(col("row_num").between(1, 5)).drop("row_num")


# Union the latest data with the forecasted data
updated_data = latest_data.union(forecast_data)

# Add "Forcasted" column with False for non-forecasted data
updated_data = updated_data.withColumn("Forcasted", when(col("Forcasted") == "True", "True").otherwise("False"))

# Insert the updated data into the hourly_weather_data Delta table
updated_data.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("fact.hourly_weather_data")

#min_created_date = latest_data.selectExpr("min(created_date)").first()[0]
#max_created_date = latest_data.selectExpr("max(created_date)").first()[0]


#return updated_data,min_created_date,max_created_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fact.hourly_weather_data;

# COMMAND ----------


