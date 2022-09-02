df3 = df2.alias('a').join(df2.alias('b'), f.col('b.String').contains(f.col('a.String')) & (f.col('a.String') != f.col('b.String')), 'leftanti')
df3.show(truncate=False)

df_names = [df1,df2,df3,df4,df5,df6,df7,df8, df9, df10, df11,df12,df13, df14,df15, df16, df17, df18,df19]
for i in df_names:
    print(i.columns)
    print((i.filter(col('ID')=='1710915368')).display())
    
  
    print(i.limit(1).display())
    
    
df2_0 = df17.select([c for c in df17.columns if c not in {'objectId', 'Name', 'expert_link', 'fullname', }])

df13.filter((lower(col("Location")).contains(lower(col("LocationAdditional"))))).display()

new = df13.filter(col('Location')!='Astana')

df17.filter(('age>90 and age<121')
CSList= CSList.dropDuplicates(['ID'])
 
demo_df\
.groupBy("driver_name")\
.agg(sum("points"), countDistinct("race_name"))\
.show()
