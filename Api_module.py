# Databricks notebook source
import requests
import uuid
from datetime import datetime
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, ArrayType
from pyspark.sql.functions import from_unixtime
from delta.tables import *
import os 
from dotenv import load_dotenv

# COMMAND ----------

dbutils.fs.cp('dbfs:/FileStore/shared_uploads/sagar.rimal@acme.edu.np/api.env', "file:/tmp/.env")
load_dotenv("/tmp/.env")    
API_key = os.getenv("API")

# COMMAND ----------

# MAGIC %run "/sagar.rimal@acme.edu.np/Weather_ETL/logger"

# COMMAND ----------

@keep_log
def get_weather(df):
    
    def fetch_weather_data(id):
        url = f'https://api.openweathermap.org/data/2.5/weather?id={id}&appid={API_key}'
        result = requests.get(url)
        return result.json()
    fetch_weather_udf = udf(lambda x: fetch_weather_data(x), weather_schema)
    
    df = df.withColumn('result', fetch_weather_udf(col('id'))).select('result')
        
    start = datetime.fromtimestamp(df.selectExpr("min(result.dt)").first()[0])
    end = datetime.fromtimestamp(df.selectExpr("max(result.dt)").first()[0])
    
    return df, start, end

# COMMAND ----------

# DBTITLE 1,Schema definition for json data 
weather_schema = StructType([
    StructField('visibility', IntegerType(), True),
    StructField('timezone', IntegerType(), True),
    StructField('main', StructType([
        StructField('temp', FloatType(), True),
        StructField('feels_like', FloatType(), True),
        StructField('temp_min', FloatType(), True),
        StructField('temp_max', FloatType(), True),
        StructField('pressure', IntegerType(), True),
        StructField('humidity', IntegerType(), True),
        StructField('sea_level', IntegerType(), True),
        StructField('grnd_level', IntegerType(), True)
    ])),
    StructField('clouds', StructType([
        StructField('all', FloatType(), True)
    ])),
    StructField('sys', StructType([
        StructField('country', StringType(), True),
        StructField('sunrise', IntegerType(), True),
        StructField('sunset', IntegerType(), True)
    ])),
    StructField('dt', IntegerType(), True),
    StructField('coord', StructType([
        StructField('lon', FloatType(), True),
        StructField('lat', FloatType(), True)
    ])),
    StructField('name', StringType(), True),
    StructField('weather', ArrayType(StructType([
                StructField('id', IntegerType(), True),
                StructField('main', StringType(), True),
                StructField('description', StringType(), True),
                StructField('icon', StringType(), True)
    ]), True)),
    StructField('cod', IntegerType(), True),
    StructField('id', IntegerType(), True),
    StructField('wind', StructType([
        StructField('speed', IntegerType(), True),
        StructField('deg', IntegerType(), True),
        StructField('gust', FloatType(), True)
    ])),
    StructField('base', StringType(), True)
])

# COMMAND ----------


