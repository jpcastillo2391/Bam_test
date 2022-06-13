# -*- coding: utf-8 -*-
"""
Testing csv to postgres on pandas
Created by Jose Castillo
Github  https://github.com/jpcastillo2391

"""
#pip install mysql-connector-python
#pip install mysql-connector-python
#pip3 install psycopg2
#Mysql libraries
import mysql.connector as connection
#postgres libraries
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
#Log Module
import logging
#for displayins in spyder variable explorer error
from IPython.display import display
#this is for malbda expression
import functools
import operator


class Connections: 
    def __init__(self):
        self.postgres_Connection()
        self.logging=logging.getLogger()
        
    # def open_Connections(self):
        
    def postgres_Connection(self):
        try:
            #psycopg2.
            postgresspass='123456789p'
            self._postgresconn = psycopg2.connect(
            database="Bam_test",
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
    
    
def main():
    logging.basicConfig(filename='ETLBamApp.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG, force=True)
    logging.getLogger()
    logging.info('Starting The application ETL in Python')

    QueryConnections = Connections()

    logging.info(str(datetime.now())+' *******************-       PARTE 1 - ETL A POSTGRES DB AUTOMATIZADO      -**********************')
    # Debe cargar las tablas incluídas en esta prueba en una base de datos local de su elección y a
    # continuación desarrollar lo que se indica en los siguientes incisos. Las soluciones deben de evitar
    # usar cursores, procedimientos almacenados o similares; se pretende que las soluciones se hagan en
    # SQL estándar. Para cada inciso, debe incluir la tabla resultante en archivo csv o excel.
    try:
    #Necesary Tables Data -  fULLY AUTOMATIZATED PROCESS -  doest care kind of separatinr in case of different formats
        separators=[',',';']
        full_tables_copy = ["datos_base_clientes","Person.Person","Production.Product","Sales.Customer","Sales.SalesOrderDetail",
                    "Secuenciales",
                    "Sales.SalesOrderHeader",
                    "Sales.SalesTerritory"
                    ]
        for table in full_tables_copy:
            for i in range(0,len(separators)):
                try:
                    print("reading csv","Test_Files/"+ table +".csv with "+separators[i])
                    table_solicitud = pd.read_csv("Test_Files/"+table + ".csv",sep=separators[i])
                    if len(table_solicitud.columns)<=1:
                            raise Exception("Only one column excel, doesnt work")
                    QueryConnections.postgres_CreateandFill(table_solicitud,table.replace(".","_").lower())
                    logging.info("Open and saver Table: "+table+".csv")
                    break
                except Exception as e:
                    print("Error****")
            
    except Exception as e:
        print("ERROR IN PROCESS READ AND LOAD INTO POSTGRES",e)
        logging.error("ERROR. Open and saver Table")


    logging.info(str(datetime.now())+'*******************-          END  ETL   MIGRATION              -**********************')


    logging.info(str(datetime.now())+'*******************-   START DATA TRANSFORMATION FOR ANALISIS  -**********************')
    logging.info(str(datetime.now())+'*******************-   INCISO 1 -**********************')
    # utilizando las tablas  SalesOrderHeader  y  SalesTerritory  escriba una consulta para calcular la
    # cantidad de transacciones y monto total mensual por territorio para cada uno de los estados según el
    # campo  Status . Los valores de  Status  son:  1  =  In process ;  2  =  Approved ;  3  =  Backordered ;  4 
    # =  Rejected ;  5  =  Shipped ;  6  =  Cancelled 
    SalesTerritory = pd.read_csv("Test_Files/Sales.SalesTerritory.csv",sep=';')
    SalesOrderHeader = pd.read_csv("Test_Files/Sales.SalesOrderHeader.csv",sep=';')
    SalesOrderHeader[["OrderDate","DueDate","ShipDate"]]=SalesOrderHeader[["OrderDate","DueDate","ShipDate"]].apply(pd.to_datetime)
    SalesOrderHeader['Mes']=SalesOrderHeader["OrderDate"].dt.strftime('%Y-%m')
    #SalesOrderHeader['Total']=SalesOrderHeader["Subtotal"]+SalesOrderHeader["Subtotal"]+SalesOrderHeader["Subtotal"]
    SalesOrderHeaderDummyfied=SalesOrderHeader.join(pd.get_dummies(SalesOrderHeader['Status']))
    SalesOrderHeaderDummyfied.rename(columns = {1:'cTrProceso', 2:'cTrAprobadas', 3:'cTrAtrasadas', 4:'cTrRechazadas', 5:'cTrEnviadas', 6:'cTrCanceladas'}, inplace = True)
    SalesOrderHeaderDummyfied['totalcTrProceso']=SalesOrderHeaderDummyfied.query('cTrProceso==1')['TotalDue']
    SalesOrderHeaderDummyfied['totalcTrAprobadas']=SalesOrderHeaderDummyfied.query('cTrAprobadas==1')['TotalDue']
    SalesOrderHeaderDummyfied['totalcTrAtrasadas']=SalesOrderHeaderDummyfied.query('cTrAtrasadas==1')['TotalDue']
    SalesOrderHeaderDummyfied['totalcTrRechazadas']=SalesOrderHeaderDummyfied.query('cTrRechazadas==1')['TotalDue']
    SalesOrderHeaderDummyfied['totalcTrEnviadas']=SalesOrderHeaderDummyfied.query('cTrEnviadas==1')['TotalDue']
    SalesOrderHeaderDummyfied['totalcTrCanceladas']=SalesOrderHeaderDummyfied.query('cTrCanceladas==1')['TotalDue']

    SalesOrderHeaderDummyfied_joined=pd.merge(SalesOrderHeaderDummyfied, SalesTerritory, on="TerritoryID")


    SalesOrderHeaderDummyfied_joined=SalesOrderHeaderDummyfied_joined.groupby(by=['Mes','Name']).agg(TrProceso=('cTrProceso', sum),
                            TrAprobadas=('cTrAprobadas', sum),TrAtrasadas=('cTrAtrasadas', sum),TrRechazadas=('cTrRechazadas', sum),
                            TrEnviadas=('cTrEnviadas', sum),TrCanceladas=('cTrCanceladas', sum),MntProceso  =('totalcTrProceso',sum),
                            MntAprobadas  =('totalcTrAprobadas',sum),MntAtrasadas  =('totalcTrAtrasadas',sum),MntRechazadas  =('totalcTrRechazadas',sum),
                            MntEnviadas  =('totalcTrEnviadas',sum),MntCanceladas  =('totalcTrCanceladas',sum))
    ###Resultado_tabla_Inciso1
    display(SalesOrderHeaderDummyfied_joined)
    SalesOrderHeaderDummyfied_joined.to_csv('Resultado-inciso1-desde-python.csv', encoding='utf-8')
    
    logging.info(str(datetime.now())+'*******************-  Fin del  INCISO 1 en python-**********************')

    
if __name__ == "__main__":
    display()
    #main()
    
    


