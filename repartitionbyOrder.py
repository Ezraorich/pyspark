ff = spark.read.format("com.crealytics.spark.excel").option("header", "true").load('/mnt/raw/Yenlik/TwitterWith_SeveralLinks.xlsx')
df2 = ff.repartition(15)
sd = df2.orderBy('sourceId', 'twitterName',)

sd.createOrReplaceTempView('main')

df = spark.sql('''
               select a.*, row_number() over (order by sourceId) as counter from {my_table} a 
        '''.format(my_table='main'))


asd = []
for i in range(0, 508520, 33901):
    if i == 508515:
        i=i+5
        asd.append(i)
    else:
        asd.append(i)
        
        
        
z = []
for i in range(len(asd)-1):
    if asd[i]==0:
        z.append((asd[i], asd[i+1]))
    else:
        z.append((asd[i]+1, asd[i+1]))
        
        
# WRITING IN CSV FORMAT:        
counter = 1
for i, j in z:
    filename = f'/mnt/raw/dump/manual_file_{counter}.xlsx'
    print(f'counter <= {j} and counter >={i}')
    df.filter(f'counter <= {j} and counter >={i}').write.format("csv").mode("overwrite").option("header","true").save(filename)
    print(filename)
    
    counter+=1
    
    
    
# WRITING IN EXCEL FORMAT:   
counter = 1
for i, j in z:
    filename = f'/mnt/raw/dump/manual_file_{counter}.xlsx'
    print(f'counter <= {j} and counter >={i}')
    swa  = df.filter(f'counter <= {j} and counter >={i}')
    swa.write.format("com.crealytics.spark.excel").option("header", "true").mode("overwrite").save(filename)
    counter+=1
    
    
    
    

