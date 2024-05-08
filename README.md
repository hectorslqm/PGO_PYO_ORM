# PGO_PYO_ORM
The DB Class in this file will let you connect to postgreSQL and SQLServer databases to Create.Read.Update.Delete. and Drop tables.
This class will do most common tasks in the database and use best practices to avoid unclosed connections and in case of error rollback transactions.

This ORM still not support 
* Joins
* Unions

### Usage
```python
from PGO_PYO_ORM import DB

database = DB()
#In case you don't want to use the default configFile
database.setConfigFileName("differentName.conf")
database.setConfigSection("server-section-in-config-file")

def create():
    insertDict = { 
        "location": "Japan", #columnName : insertValue
        "columnA" : 3
    }
    #From TableName this will set the values of updateDict according to the condition
    database.update("tabelName", insertDict, showQuery = True) #last arg will print the created query in console

def read():
    selectDict = {
        "tableName" : "yourDBTableName",
        "select" : "name, address, count(*)",#Optional argument (by omitting will retrieve all columns)
        "condition" : "location = 'United States'", #optional argument
        "groupBy" : "name, adress", #optional argument
        "orderBy" : "name ASC", #optional argument
        "limit" : 3 #optional argument
        
    }
    #returns a jsonObject that contains the results of the given selection
    return results = database.select(selectDict)
    
def update(): 
    updateDict = { 
        "location": "Europe", #columnName : newValue
        "columnA" : 3
    }
    condition = "location like '%Mexico%'"
    #From TableName this will set the values of updateDict according to the condition
    database.update("tabelName", updateDict, condition, showQuery = True) #last arg will print the created query in console

def delete():
    condition = "name like '%John Doe%'"
    #From tableName This will delete each record which contains John Doe in name
    database.delete("tableName", condition, showQuery = True) #last arg will print the created query in console

def dropTable():
    database.dropTable("tableName")
    return "table deleted"

def createTable():
    argsDict = {
        "id" : "SERIAL PRIMARY KEY",
        "updated_timestamp" : "timestamp default timezone('UTC'::text, CURRENT_TIMESTAMP) NOT NULL",
        "location" : "CHAR(45) NOT NULL",
        "name" : "CHAR(100) NOT NULL",
        "date" : "timestamp NULL"
    } 
    database.createTable("tableName", argsDict, overWrite = True, showQuery = False) #overWrite will drop a table that match the name of the current table and creates a new one with argsDict Characteristics
    return "table created"
```
### Configurations
|  REQUIREMENTS    |    TYPE   |
|------------------|:---------:|
|**PGO_PYO_ORM.py**|python file|
|**dbconfig.conf** |config file|
|psycopg2          |py module  |
|psycopg2-binary   |py module  |
|PYODBC            |py module  |
|ODBC SQL 18       |Driver     |
#### dbconfig.conf
This file has an example of how it is suppose to be configured. **Use your own configurations**
#### To install psycopg2
Run the following commands
```bash
pip install psycopg2
pip install psycopg2-binary
```
#### To install pyodbc
Run the following command  `pip install pyodbc`
#### To install ODBC SQL 18 on windows
Install the following driver [Microsoft SQL ODBC DRIVER 18](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16)
#### To install ODBC SQL 18 on linux
Run the following commands
```bash
sudo curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
sudo curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list
sudo curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo apt-get install -y msodbcsql18
```
#### To install ODBC SQL 18 on mac
You must have [**Homebrew**](https://brew.sh/) and then Run the following commands
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update
HOMEBREW_ACCEPT_EULA=Y brew install msodbcsql18 mssql-tools18
```

