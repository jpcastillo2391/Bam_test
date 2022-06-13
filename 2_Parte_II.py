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
#libreria necesaria para los textboxes
import tkinter as tk
from tkinter import filedialog
from tkinter import simpledialog
from tkinter import messagebox

import os
#talvez vayas a necesitar instalar esta libreria
from pandastable import Table, TableModel

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
    
class TestApp(tk.Frame):            
    def __init__(self, parent=None,dataframe=None):            
        self.parent = parent            
        Frame.__init__(self)            
        self.main = self.master            
        self.main.geometry('600x400+200+100')            
        self.main.title('Table app')            
        f = Frame(self.main)            
        f.pack(fill=BOTH,expand=1)            
        df = TableModel.getSampleData()            
        self.table = pt = Table(f, dataframe=df,                                    
        showtoolbar=True, showstatusbar=True)            
        pt.show()            
        returnapp = TestApp()    
    
def main():
    application_window = tk.Tk()
    application_window.title(string='Bamapp Dashboard')
    
    log_answer  = filedialog.asksaveasfilename(parent=application_window,
                                      initialdir=os.getcwd(),
                                      title="Por favor, selecciona un archivo para guardar tu bitacora",
                                      defaultextension=".log",
                                      initialfile="Parte2_log.log",
                                      filetypes=(("Bamapp Log file", "*.log"),("All Files", "*.*") ))
    Db_answer = simpledialog.askstring("Input", "Ingrese Database a conectarse?",
                                parent=application_window,
                                initialvalue="Bam_test")
    Table_answer = simpledialog.askstring("Input", "Ingrese Nombre del Catalogo/Tabla () donde almacenara el resultado ?",
                                parent=application_window,
                                initialvalue="Bam_test_table")
    Script_location = filedialog.askopenfilename(parent=application_window,
                                    initialdir=os.getcwd(),
                                    title="Seleccione .sql conteniendo el Script a ejecutar :",
                                    initialfile="Script_prueba_ejecucion.sql",
                                    filetypes=(("SQl files", "*.sql"),("All Files", "*.*") ))
    
    logging.basicConfig(filename=log_answer, filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG, force=True)
    logging.getLogger()
    logging.info('Starting The application Script Executer in Python')
    logging.info(str(datetime.now())+' *******************-       PARTE 2 - Carga de .sql script desde  python    -**********************')

    try:
        '''
           Me voy a conectar a la db y a ejecutar el script
        '''
        QueryConnections = Connections(Db_answer)
        fd = open(Script_location, 'r')
        sqlFile = fd.read()
        fd.close()
        print(sqlFile)
        mydataframe=QueryConnections.postgres_executer(sqlFile)
        print(mydataframe)
        
        '''
           Voy a mostrar el datatable en una ventana extra , para poder visualizarlo
        '''
        frame = tk.Frame(application_window)
        frame.pack(fill='both', expand=True)
        
        pt = Table(frame)
        pt.show()
        pt.model.df = mydataframe
        #pt.model.df = TableModel.getStackedData()
        
        logging.info(str(datetime.now())+'*******************-  CIERRE ALA VENTANA DEL EXCEL PARA CONTINUAR-**********************')
        application_window.mainloop()
        print("CERRO LA VENTANA")
        
        '''
            Procedo a almacenarlo en la tabla indicada.
        '''
        QueryConnections.postgres_CreateandFill(mydataframe,Table_answer)
        
    except Exception as e:
        logging.error(str(datetime.now())+'Error dento de la ejecucion')
    
    
    logging.info(str(datetime.now())+'*******************-  Fin del  INCISO 1 en python-**********************')
    
    #application_window.destroy()
        
if __name__ == "__main__":
    display()
    main()
    
    


