
## SPLIT COLUMNS:

hg.withColumn("temp", split(col("fullname"), ",")).select(col("temp").getItem(1).alias('credentials'), 'NPI')


from pyspark.sql.functions import regexp_replace
df = df.withColumn('ln_specialty1', regexp_replace('ln_specialty1', ',', ''))




# SELECT THE LAST ITAM IN THE STRIJNG COLUMN:
addr = address1.withColumn("a1", split(col("address1"), " ")).select( col("a1").getItem(size(col("a1"))-1).alias('zip'), 'NPI')
