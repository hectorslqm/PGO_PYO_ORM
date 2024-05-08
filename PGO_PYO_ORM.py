import time
import psycopg2
import pyodbc
from typing import Union
from configparser import ConfigParser

HOST = 'host'
POSTGRES = 'postgres'

class DB:
    '''
    This class works with pyodbc or psycopg databases
    '''
    def __init__(self,configFileName: str = 'dbconfig.conf', configSection: str = 'yourSection'):
        self._configFileName = configFileName
        self._configSection = configSection
    
    def setConfigFileName(self, value: str):
        self._configFileName = value
        
    @property
    def getConfigFileName(self) -> str:
        return self._configFileName
    
    def setConfigSection(self, value: str):
        self._configSection = value
        
    @property
    def getConfigSection(self) -> str:
        return self._configSection
    
    def select(self, selectDict: dict, showQuery: bool = False) -> dict:
        '''
        Create a query for the given ``selectDict`` tableName is the only mandatory key
        
        ``selectDict`` : {
            ``tableName``: target table
            ``select``: columns to retrieve, don't use this key to retrieve all
            ``condition``: Where filters, E.G. condition : "name = 'yourName'  and age > 12 "
            ``orderBy`` : E.G. "name ASC"
            ``groupBy`` : Group by any
            ``limit`` : in case you want to limit the results
        }
        ``showQuery`` : if True print the query in console
        '''
        results =[]
        conn = None
        start_time = time.time()
        tableName = selectDict.get('tableName','')
        select = selectDict.get('select', '*')
        condition = selectDict.get('condition', None)
        groupBy = selectDict.get('groupBy', None)
        orderBy = selectDict.get('orderBy', None)
        limit = selectDict.get('limit', None)
        try:
            params = config(self)
            isPostgres = POSTGRES in params[HOST]
            conn = psycopg2.connect(**params) if isPostgres else pyodbc.connect(**params)
            cursor = conn.cursor()
            
            #verify if the table exists
            cursor.execute("select exists(select * from information_schema.tables where LOWER(table_name)=LOWER(%s))", (f'{tableName}',))
            if cursor.fetchone()[0]:
                #READ ALL
                query = ["SELECT {} FROM {}"]
                values = [select, tableName]
                if condition:
                    query.append("WHERE {}")
                    values.append(condition)
                if groupBy:
                    query.append("GROUP BY {}")
                    values.append(groupBy)
                if orderBy:
                    query.append("ORDER BY {}")
                    values.append(orderBy)
                if limit:
                    query.append("LIMIT {}")
                    values.append(limit)
                
                if showQuery:
                    print(f"Select Query: {' '.join(query).format(*values)}")
                cursor.execute(' '.join(query).format(*values))
                
                headers = [x[0] for x in cursor.description]
                if headers == 0:
                    return results
                
                for row in cursor.fetchall():
                    result = {}
                    for content, header in zip(row, headers):
                        if isinstance(content, str):
                            result[header] = content.strip(" ")
                        else:
                            result[header] = content
                    results.append(result)
                affected_rows = cursor.rowcount 
                return results
            else:
                raise Exception(f"Table {tableName} Doesn't Exist")
                                
        except (Exception, psycopg2.Error, pyodbc.Error) as error:
            print("Error While Fetching: ", error)
            return []
            
        finally:
            # closing database connection.
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                end_time = time.time()
                elapsed_time = end_time - start_time
                minutes, seconds = divmod(elapsed_time, 60)
                print(f"{affected_rows} Fetched Rows")
                print(f"DB Connection Closed. Elapsed time {int(minutes)} minutes and {seconds:.2f} seconds")
    
    def update(self, tableName: str, updateDict: dict, conditionStr: str, showQuery: bool = False):
        '''
        Update data in a table.
        ``tableName``: target table
        ``updateDict``: {
            ``columnToUpdate``: update value,
            ...
            ``columnToUpdateN`` : update value n
        }
        ``conditionStr``: where clause
        ``showQuery``: if True print the query in console
        '''
        startTime = time.time()
        conn = None
        try:  
            params = config(self)
            isPostgres = POSTGRES in params[HOST]
            conn = psycopg2.connect(**params) if isPostgres else pyodbc.connect(**params)
            cursor = conn.cursor()
            
            cursor.execute("select exists(select * from information_schema.tables where table_name=LOWER(%s))", (f'{tableName}',))
            if cursor.fetchone()[0]:
                set_clause = ", ".join([f"{key} = '{value}'" if isinstance(value, str) else f"{key} = {value}" for key, value in updateDict.items()])
                query = "UPDATE {} SET {} WHERE {}"
                if showQuery:
                    print(f"Update Query: {query.format(tableName, set_clause, conditionStr)}")
                cursor.execute(query.format(tableName, set_clause, conditionStr))
                updatedRows = cursor.rowcount
            else:
                print(f"Table {tableName} Doesn't Exist")
            conn.commit()
            
        except (Exception, psycopg2.Error, pyodbc.Error) as error:
            print("Error While Updating Rows: ", error)
            print("Rollback Transaction")
            if conn:
                conn.rollback()
                
        finally:
            # closing database connection.
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                endTime = time.time()
                elapsedTime = endTime - startTime
                minutes, seconds = divmod(elapsedTime, 60)
                print(f"{updatedRows} Updated Rows")
                print(f"DB Connection Closed. Elapsed time {int(minutes)} minutes and {seconds:.2f} seconds") 
    
    def insert(self, tableName: str, insertDict: dict, showQuery: bool = False):
        '''
        ``tableName``: target table
        ``insertDict``: {
            ``columnName``: insert value,
            ...
            ``columnNameN`` : insert value n
        }
        ``showQuery``: if True print the query in console
        '''
        startTime = time.time()
        conn = None
        try:  
            params = config(self)
            isPostgres = POSTGRES in params[HOST]
            conn = psycopg2.connect(**params) if isPostgres else pyodbc.connect(**params)
            cursor = conn.cursor()
            
            cursor.execute("select exists(select * from information_schema.tables where table_name=LOWER(%s))", (f'{tableName}',))
            if cursor.fetchone()[0]:
                columns = ", ".join(insertDict.keys())
                values = ", ".join([f"'{value}'" if isinstance(value, str) else str(value) for value in insertDict.values()])
                query = f"INSERT INTO {tableName} ({columns}) VALUES ({values})"
                if showQuery:
                    print(f"Insert Query: {query}")
                cursor.execute(query)
                insertedRows = cursor.rowcount
            else:
                print(f"Table {tableName} Doesn't Exist")
                
            conn.commit()
            
        except (Exception, psycopg2.Error, pyodbc.Error) as error:
            print("Error While Inserting new rows: ", error)
            print("Rollback Transaction")
            if conn:
                conn.rollback()

        finally:
            # Cerrar conexión y cursor
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                endTime = time.time()
                elapsedTime = endTime - startTime
                minutes, seconds = divmod(elapsedTime, 60)
                print(f"{insertedRows} Inserted Rows.")
                print(f"DB Connection Closed. Elapsed time {int(minutes)} minutes and {seconds:.2f} seconds") 
                  
    def delete(self, tableName: str, conditionStr: str, showQuery : bool = False):
        ''''
        Delete data in a table.
        ``tableName``: target table
        ``conditionStr``: where clause
        ``showQuery``: if True print the query in console
        '''
        startTime = time.time()
        conn = None
        try:  
            params = config(self)
            isPostgres = POSTGRES in params[HOST]
            conn = psycopg2.connect(**params) if isPostgres else pyodbc.connect(**params)
            cursor = conn.cursor()
            cursor.execute("BEGIN READ WRITE")
            cursor.execute("select exists(select * from information_schema.tables where table_name=LOWER(%s))", (f'{tableName}',))
            if cursor.fetchone()[0]:
                query = "DELETE FROM {} WHERE {}"
                if showQuery:
                    print(f"Delete Query: {query.format(tableName, conditionStr)}")
                cursor.execute(query.format(tableName, conditionStr))
                deletedRows = cursor.rowcount 
            else:
                raise Exception(f"Table {tableName} Doesn't Exist")
            conn.commit()
            
        except (Exception, psycopg2.Error, pyodbc.Error) as error:
            print("Error While Deleting Rows: ", error)
            print("Rollback Transaction")
            if conn:
                conn.rollback()
                
        finally:
            # closing database connection.
            if cursor:
                    cursor.close()
            if conn:
                conn.close()
                endTime = time.time()
                elapsedTime = endTime - startTime
                minutes, seconds = divmod(elapsedTime, 60)
                print(f"{deletedRows} Deleted Rows")
                print(f"DB Connection Closed. Elapsed time {int(minutes)} minutes and {seconds:.2f} seconds") 
    
    def createTable(self, tableName, argsDict, overWrite=False, showQuery: bool = False):
        ''''
        Create Table
        ``tableName``: target table
        ``argsDict``: {
            ``columnName``: column characteriztics as string E.G. "timestamp default timezone('UTC'::text, CURRENT_TIMESTAMP) NOT NULL"
        }
        ``overWrite``: if True drops the table named as the ``tableName`` and creates a new one. BE CAREFUL USING THIS AS TRUE
        ``showQuery``: if True print the query in console
        '''
        startTime = time.time()
        conn = None
        try:
            params = config(self)
            isPostgres = POSTGRES in params[HOST]
            conn = psycopg2.connect(**params) if isPostgres else pyodbc.connect(**params)
            cursor = conn.cursor()

            cursor.execute("BEGIN READ WRITE")
            cursor.execute("select exists(select * from information_schema.tables where table_name=LOWER(%s))", (f'{tableName}',))
            
            # build query
            columns = ", ".join([f"{column_name} {data_type}" for column_name, data_type in argsDict.items()])
            query = f"CREATE TABLE {tableName} ({columns})"
            if showQuery:
                print(f"Create Table Query: ")
            # Verify if table exists already
            table_exists = cursor.fetchone()[0]
            if table_exists:
                if overWrite:
                    print(f"Dropping Table {tableName}")
                    cursor.execute(f"DROP TABLE {tableName}")
                    print(f"Creating Table {tableName}")
                    cursor.execute(query)
                else:
                    print("Table Already Exists, To Drop Previous table and create a new one use 'overWrite' True.")
            else:
                print(f"Creating Table {tableName}")
                cursor.execute(query)
            
            conn.commit()

        except (Exception, psycopg2.Error, pyodbc.Error) as error:
            print("Error while creating table:", error)
            print("Rollback Transaction")
            if conn:
                conn.rollback()

        finally:
            # Cerrar la conexión y el cursor
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                endTime = time.time()
                elapsedTime = endTime - startTime
                minutes, seconds = divmod(elapsedTime, 60)
                print(f"DB Connection Closed. Elapsed time {int(minutes)} minutes and {seconds:.2f} seconds") 
    
    def dropTable(self, tableName, showQuery: bool = False):
        '''
        Drop a Table. ``BE CAREFUL USING THIS!``
        ``tableName``: target table
        ``showQuery``: if True print the query in console
        '''
        startTime = time.time()
        conn = None
        try:
            params = config(self)
            isPostgres = POSTGRES in params[HOST]
            conn = psycopg2.connect(**params) if isPostgres else pyodbc.connect(**params)
            cursor = conn.cursor()

            cursor.execute("BEGIN READ WRITE")
            cursor.execute("select exists(select * from information_schema.tables where table_name=LOWER(%s))", (f'{tableName}',))
            table_exists = cursor.fetchone()[0]

            if table_exists:
                query = f"DROP TABLE {tableName}"
                if showQuery:
                    print(f"Drop Table Query: {query}")
                cursor.execute(query)
            else:
                print(f"Table {tableName} Doesn't Exist")

            conn.commit()

        except (Exception, psycopg2.Error, pyodbc.Error) as error:
            print("Error while dropping table:", error)
            if conn:
                conn.rollback()

        finally:
            # Cerrar la conexión y el cursor
            if cursor:
                cursor.close()
            if conn:
                conn.close()
                endTime = time.time()
                elapsedTIme = endTime - startTime
                minutes, seconds = divmod(elapsedTIme, 60)
                print("Dropped Table")
                print(f"DB Connection Closed. Elapsed time {int(minutes)} minutes and {seconds:.2f} seconds") 
                           
def config(db: DB):
        parser = ConfigParser()
        parser.read(db.getConfigFileName)
        databaseDict = {}
        if parser.has_section(db.getConfigSection):
            params = parser.items(db.getConfigSection)
            for param in params:
                databaseDict[param[0]] = param[1]
        else:
            raise Exception(f"Section {db.getConfigSection} not found in the {db.getConfigSection} file")

        return databaseDict  
