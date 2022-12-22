import pyspark.sql.utils
try:
    for i in range(len(need_path)):
    
        print(i,'  = ', need_path[i])
        df = spark.read.parquet(need_path[i])
        df.filter(col('NCTId')=='NCT02234063').display()
    
    print ("Query executed")
except pyspark.sql.utils.AnalysisException:
    for i in range(len(need_path)):
        print('JOIN:', i,'  = ', need_path[i])
        df = spark.read.parquet(need_path[i])
        df = df.drop('NCTId')
        new = CT.join(df, on='NCTCodePBI', how = 'inner')
        new.filter(col('NCTId')=='NCT02234063').display()
    
    #print("Unable to process your query dude!!")
    
    
    
