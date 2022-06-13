# -*- coding: utf-8 -*-
"""
Created on Mon Jun 13 00:56:52 2022

Testing csv to postgres on pandas
Created by Jose Castillo
Github  https://github.com/jpcastillo2391

"""
# mi databricks ejecuta spark 3.2.1
# pip install -U "databricks-connect==7.3.*4"  #no usen esta
# pip install -U “databricks-connect==10.4.0b0”
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date
# recordar instalar pyspark en el virtual enviroment, visualcode no lo esta agarrando. revisar conda env
sc = pyspark.SparkConf()

spark = SparkSession.builder.appName('temps-demo').getOrCreate()

# Create a Spark DataFrame consisting of high and low temperatures
# by airport code and date.
schema = StructType([
    StructField('AirportCode', StringType(), False),
    StructField('Date', DateType(), False),
    StructField('TempHighF', IntegerType(), False),
    StructField('TempLowF', IntegerType(), False)
])

data = [
    ['BLI', date(2021, 4, 3), 52, 43],
    ['BLI', date(2021, 4, 2), 50, 38],
    ['BLI', date(2021, 4, 1), 52, 41],
    ['PDX', date(2021, 4, 3), 64, 45],
    ['PDX', date(2021, 4, 2), 61, 41],
    ['PDX', date(2021, 4, 1), 66, 39],
    ['SEA', date(2021, 4, 3), 57, 43],
    ['SEA', date(2021, 4, 2), 54, 39],
    ['SEA', date(2021, 4, 1), 56, 41]
]

temps = spark.createDataFrame(data, schema)

# Create a table on the Databricks cluster and then fill
# the table with the DataFrame's contents.
# If the table already exists from a previous run,
# delete it first.
spark.sql('USE default')
spark.sql('DROP TABLE IF EXISTS demo_temps_table')
temps.write.saveAsTable('demo_temps_table')

# Query the table on the Databricks cluster, returning rows
# where the airport code is not BLI and the date is later
# than 2021-04-01. Group the results and order by high
# temperature in descending order.
df_temps = spark.sql("SELECT * FROM demo_temps_table "
                     "WHERE AirportCode != 'BLI' AND Date > '2021-04-01' "
                     "GROUP BY AirportCode, Date, TempHighF, TempLowF "
                     "ORDER BY TempHighF DESC")
df_temps.show()

# Results:
#
# +-----------+----------+---------+--------+
# |AirportCode|      Date|TempHighF|TempLowF|
# +-----------+----------+---------+--------+
# |        PDX|2021-04-03|       64|      45|
# |        PDX|2021-04-02|       61|      41|
# |        SEA|2021-04-03|       57|      43|
# |        SEA|2021-04-02|       54|      39|
# +-----------+----------+---------+--------+

# Clean up by deleting the table from the Databricks cluster.
spark.sql('DROP TABLE demo_temps_table')
