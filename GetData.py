# from pyspark.sql import SQLContext
# from pyspark import SparkContext
# sc=SparkContext("local", "test")
# sqlContext = SQLContext(sc)

# df = sqlContext.read.format('com.databricks.sparki.csv').options(head = 'true', inferschema = 'true').load(path='./archive/Chicago_Crimes_2001_to_2004.csv')
from cassandra.cluster import Cluster
import pandas as pd
def readCSV():
    df = pd.read_csv('./archive/Chicago_Crimes_2001_to_2004.csv', error_bad_lines=False)
    df = df.dropna()
    print(df)
    return df

def toCassandra(dataFrame):
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect('cmpe266')
    query = "INSERT INTO chicago(id, arrest, block, date, description, domestic, location_description, primary_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
    prepared = session.prepare(query)
    for index, row in dataFrame.iterrows():
        session.execute(prepared, (str(row['ID']),str(row['Arrest']), str(row['Block']), str(row['Date']), str(row['Description']),str(row['Domestic']), str(row['Location Description']), str(row['Primary Type'])))

    print("completed")

df = readCSV()
toCassandra(df)



