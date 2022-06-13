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
import psycopg2
from psycopg2 import connect
#Dataframes manipularion
import pandas as pd
import io
#for postgres integration
from sqlalchemy import create_engine
# import Python's JSON library to format JSON strings
import json
#This is for getting timing for logs
from datetime import datetime
import logging



class Connections: 
    def __init__(self,databasein=None):
        self.postgres_Connection(databasein)
        self.logging=logging.getLogger()
        
    # def open_Connections(self):
        
    def postgres_Connection(self,databasein="Bam_test"):
        try:
            #I assume for this instance, that the password 123456789p will be the same, and all is in the same db
            postgresspass='123456789p'
            self._postgresconn = psycopg2.connect(
            database=databasein,
            user='postgres',
        	password=postgresspass,
        	host='localhost',
        	port='5432'
            )
            self.dsn=self._postgresconn.get_dsn_parameters()
            self.dsn['password']=postgresspass
        except Exception as e:
            print("No Connection to Postgres")
            
    
            
    
    def postgres_CreateandFill(self,resultDF,TableName):
        try:
            print("******************************************************************************")
            resultDF.columns=resultDF.columns.str.lower()
            '''
               Programacion defensiva :  Tablas con palabras reservadas para postgres como name y group
            '''
            for i in range(0,len(resultDF.columns)):
                if "group" in resultDF.columns.values[i].lower():
                    resultDF.columns.values[i] = resultDF.columns.values[i].lower()+"_t"
                if "name" in resultDF.columns[i].lower():
                    resultDF.columns.values[i] = resultDF.columns.values[i].lower()+"_t"
            self.mysql_CreateTableHeader_intoPostgres(resultDF,TableName)
            self.postgres_fillinTable(resultDF,TableName)
            print("******************************************************************************")
        except psycopg2.ProgrammingError as exc:
            print ("Error due Create and fill" +exc.message)
            self.logging.error("Error on CreateandFill")
        except psycopg2.InterfaceError as exc:
            self.postgres_Connection()
            print (exc.message+ " Error on connection, trying again..")
            self.logging.error("Error on connection, trying again..")
            self.postgres_CreateandFill(resultDF,TableName)
        except Exception as w:
            print(w)
    
    '''
        No deberia de llamarse mysql into postgres, pero esta funcion la use en su momento para sincronizar bases de datos
    '''
    def mysql_CreateTableHeader_intoPostgres(self,resultDF,TableName):
        try:
            columnName = list(resultDF.columns.values)
            #print(resultDF.dtypes)
            columnDataType = self.getColumnDtypes(resultDF.dtypes)
            #uncomment if you want to check the columns properties
            #print(columnName,columnDataType,len(columnName),len(columnDataType))
            createTableStatement = 'DROP TABLE IF EXISTS '+TableName+' ; CREATE TABLE IF NOT EXISTS '+TableName+' ('
            for i in range(len(columnDataType)):
                if "date" in columnName[i].lower():
                    columnDataType[i]="timestamp"
                createTableStatement = createTableStatement + '\n' + columnName[i].lower().replace(" ","") + ' ' + columnDataType[i] + ','
            createTableStatement = createTableStatement[:-1] + ' );'
            ##UNCOMMENT IF YOU WANT TO CHECK THE CREATION QUERY
            ##print(createTableStatement)
            cur = self._postgresconn.cursor()
            cur.execute(createTableStatement)
            self._postgresconn.commit()
        except psycopg2.DatabaseError as error:
            print("DatabaseError: "+error)
            self.logging.error("Error: in Create table header" + TableName)
        except Exception as e:
            print(e)
        if self._postgresconn is not None:
            print(str(datetime.now())+"\t"+TableName+" Header created")
            self.logging.info(str(datetime.now())+"\t"+TableName+" Header created")
            
    def postgres_executer(self,script):
        try:
            # Using alchemy method
            connect_alchemy = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
                self.dsn['user'],
                self.dsn['password'],
                self.dsn['host'],
                self.dsn['port'],
                self.dsn['dbname']
            )
            print ('Peparing Engine for Script Executing:')
            self.logging.debug(str(datetime.now())+"\t"+"Peparing Engine for Script Executing: ")
            engine = create_engine(connect_alchemy, client_encoding="utf8", pool_pre_ping=True)
            resultDF = pd.read_sql_query(script,engine)
            
        except Exception as e:
            print("ERROR" + str(e))
            self.logging.error(str(datetime.now())+"\t"+"Error on Finish executing... ")
        finally:
            print (str(datetime.now())+"\t"+"Finish Executing... ")
            self.logging.info(str(datetime.now())+"\t"+"Finish Executing... ")
        return resultDF
    
    def postgres_fillinTable(self,resultDF,TableName):
        try:
            # Using alchemy method
            connect_alchemy = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (
                self.dsn['user'],
                self.dsn['password'],
                self.dsn['host'],
                self.dsn['port'],
                self.dsn['dbname']
            )
            print (str(datetime.now())+"\t"+"Filling: "+TableName)
            self.logging.debug(str(datetime.now())+"\t"+"Filling: "+TableName)
            engine = create_engine(connect_alchemy, client_encoding="utf8", pool_pre_ping=True)
            resultDF.to_sql(TableName, con=engine, index=False,if_exists='append',chunksize = 5000,method='multi' )
            
        except Exception as e:
            print("ERROR" + str(e))
            self.logging.error(str(datetime.now())+"\t"+"Error on Finish... "+TableName)
        finally:
            print (str(datetime.now())+"\t"+"Finish... "+TableName)
            self.logging.info(str(datetime.now())+"\t"+"Finish... "+TableName)
    
    def getColumnDtypes(self,dataTypes):
        dataList = []
        for x in dataTypes:
            if(x == 'int64'):
                dataList.append('bigint')
            elif (x == 'float64'):
                dataList.append('float')
            elif (x == 'bool'):
                dataList.append('boolean')
            elif (x == 'datetime64[ns]'):
                dataList.append('timestamp')
            else:
                dataList.append('varchar')
        return dataList

logging.basicConfig(filename='Parte3Log.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG, force=True)
logging.getLogger()
logging.info('Starting The application ETL in Python')
QueryConnections = Connections("Bam_test")
mydataframe=QueryConnections.postgres_executer("SELECT * FROM datos_base_clientes ")
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

temps = spark.createDataFrame(mydataframe)

# Create a table on the Databricks cluster and then fill
# the table with the DataFrame's contents.
# If the table already exists from a previous run,
# delete it first.
spark.sql('USE default')
spark.sql('DROP TABLE IF EXISTS datos_base_clientes')
temps.write.saveAsTable('datos_base_clientes')

# Query the table on the Databricks cluster, returning rows
# where the airport code is not BLI and the date is later
# than 2021-04-01. Group the results and order by high
# temperature in descending order.
df_temps = spark.sql("SELECT * FROM datos_base_clientes ")
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
#spark.sql('DROP TABLE demo_temps_table')
