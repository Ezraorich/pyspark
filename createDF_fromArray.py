keys = ['G_id', 'randomId', 'randomId_old', 'randomId_DOX']
array = []  
for i in range(len(path_names)):
     
    df = spark.read.parquet(path_names[i])
    all_ck = [column for column in df.columns]
        
    filename = path_names[i].split('/')[11]
    print(filename)
 
    result = set(keys) & set(all_ck) 
    final = list(result)
    print(list(result))
    for h in df.columns:
        inner_array = []
        inner_array.append(filename)
        inner_array.append(h)
        inner_array.append(df.select(col(h)).distinct().count())
        inner_array.append(df.filter(col(h).isNotNull()).select(final).distinct().count()/df.select(final).distinct().count()*100)
        array.append(inner_array)
        
    
    
        
        
        
df_stats = spark.createDataFrame(data=array, schema = ["TableName","ColumnName", 'distinctValuesCount', 'Coverage'])
df_stats.display()
excel_filename = f'/mnt/workplace/Saltanat/mapping_Stats.xlsx'
print(excel_filename)
df_stats.write.format("com.crealytics.spark.excel").option("header", "true").mode("overwrite").save(excel_filename)
