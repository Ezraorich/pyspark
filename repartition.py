A = df4.filter(df4.PubDate_Year.isNotNull()).select('PubDate_Year')
A = A.repartition(4*4*8).persist()
A.count()
B = df4.filter(df4.ArticleDate_Year.isNotNull()).select('ArticleDate_Year')
B = B.repartition(4*4*8).persist()
B.count()


d = df4.select('Pub_Year','PubDate_MedDate', 'Article_Year')

coverage = d.count()-d.filter((col('Pub_Year').isNull())&(col('PubDate_MedDate').isNull())& (col('Article_Year').isNull()) ).count() 
print('Pub_Year, Article_Year and PubDate_MedDate filled amount: ',coverage)
print('Overall number:',d.count())
print('Percentage of coverage with Pub_Year, Article_Year and PubDate_MedDate:',(coverage/d.count())*100)

# select 4 letters in df column:
new = df4.withColumn('PubDate_Medline_year', df4.PubDate_MedlineDate.substr(1, 4))


dd1 = dd1.withColumn('Result', ( dd1['A'] - dd1['B'] ) / dd1['A'] )

df4.write.parquet("/mnt/raw/Data/SaltanatTest/",mode="overwrite")
