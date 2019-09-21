#Import a file with more than 50K records under 1 minutes

#Import necessary libraries
import pandas as pd
import csv
import glob
import time
import pyodbc
import datetime

#Define functions to split data into smaller chunks
def CreateTheValues(value):
    #Encapsulate the input for SQL use (add ' etc)
    if isinstance(value, list):
        r = []
        for i in value:
            if isinstance(i, basestring):
                r.append(encapsulate(i))
            else:
                r.append(str(encapsulate(i)))
        return ','.join(r)
    elif isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    elif isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
        return "'" + value.isoformat() + "'"
    elif value is None:
        return "Null"
    else:
        return str(value)
        
def SplitTheTuple(rows):
    clLists = []
    cl = []
    LineCounter = 0
    for i in rows:
        if LineCounter >= 1000:
            clLists.append(",".join(cl))
            cl = []
            LineCounter = 0
        cl.append("" + CreateTheValues(i) + "")
        LineCounter += 1
    clLists.append(",".join(cl))
    return clLists


#Define dataframes
df = pd.DataFrame()
consolidatedDf = pd.DataFrame()


path = "C:/Users/ajinkya.shingne/Downloads/New folder (3)/*.csv"
for filename in glob.glob(path):
    with open(filename, 'r') as f:
        df = pd.read_csv(f,header=0)
        consolidatedDf = consolidatedDf.append(df)

#Connection Details for Azure SQL Database

server = <Server Name>
database = <DataBase Name>
username = <User Name>
password = <Password>
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD=' + password, autocommit=True)
cursor = cnxn.cursor()

cursor.fast_executemany = True 


#Convert the data frame to tuple
ConvertedTuple = list(consolidatedDf.itertuples(index=False, name=None))

#Start Timer
StartTime = time.time()

#Insert records
for data in SplitTheTuple(ConvertedTuple):
    cursor.execute("Insert into [dbo].[MyTable] (column1,column2,column3,column4,column5) Values " + data+";")
cursor.commit()
cursor.close()

#Print Success message
print("The migration is completed successfully.")

#Clock the time
print("It took "+ str(time.time()-StartTime)+" seconds to complete")
