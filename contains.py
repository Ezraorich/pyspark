df3 = hospital.alias('a').join(pl.alias('b'), f.col('b.practiceLocationAdditionalName').contains(f.col('a.hospitalName')))

df.filter(col("name").contains("mes")).show()


from pyspark.sql.functions import col
df.filter(col("name").contains("mes")).show()

df.filter(col("name").like("%mes%")).show()
