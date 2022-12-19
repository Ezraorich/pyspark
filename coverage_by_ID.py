# you have path with several csv files. each file can be converted to parquet tables.
# each table has column which contains ID, if you want to count the coverage for thar several parquet tables iteratively by ID, you can you this code.

PATH  = '/mnt/raw/saltanatkhalyk/IndividualHospitals/'

path_names = [] 
for i in dbutils.fs.ls(PATH):
    path_names.append(i[0])




# For national

by_ID  = 'ID'
        
        
import pandas as pd
from pyspark.sql.functions import col
array = []
for i in range(len(path_names)):
    df  = spark.read.option('header', True).csv(path_names[i])
    all_columns = [column for column in df.columns]
    for column_names in all_columns:
        if by_ID in column_names:
            print(column_names)
            selected_column = column_names
            
    for j in df.columns:
        inner_array = []
        inner_array.append(path_names[i].split('/')[-1][:-4])
        inner_array.append(j)
        inner_array.append(df.filter(col(j).isNotNull()).select(selected_column).distinct().count()/df.select(selected_column).distinct().count()*100)
        inner_array.append(df.select(col(j)).distinct().count())
        array.append(inner_array)

    
display(pd.DataFrame(array, columns=['Table name','Column_name','Coverage', 'Distinct count']))
