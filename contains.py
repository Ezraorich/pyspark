df3 = hospital.alias('a').join(pl.alias('b'), f.col('b.practiceLocationAdditionalName').contains(f.col('a.hospitalName')))

df.filter(col("name").contains("mes")).show()


from pyspark.sql.functions import col
df.filter(col("name").contains("mes")).show()

df.filter(col("name").like("%mes%")).show()from pyspark.sql import functions as f



from pyspark.sql import functions as f

df3 = hospital.alias('a').join(pl.alias('b'), f.col('b.practiceLocationAdditionalName').contains(f.col('a.hospitalName')) & (f.col('a.hospitalName') != f.col('b.practiceLocationAdditionalName')), 'leftanti')
df3.show(truncate=False)

pl.filter((lower(col("practiceLocationAdditionalName")).contains(lower(col("practiceLocationMainName"))))).dropDuplicates(['N']).count()


S= df14.select('formattedDate').withColumn('length', 
                F.length(col('formattedDate')))


CList.alias('l').join(dfPI.alias('r'), on='physician').count()

photo  = photo.select('imageURL', 'N').where(col("imageURL").isNotNull())

ss =CMSList.select('primarySpecialty','primarySubSpecialty','N','firstName','lastName').distinct().where((col("primarySpecialty").isNotNull())&col("primarySubSpecialty").isNotNull())


Cccc.subtract(Hhhh).display()

Au.select('Code').filter(col("Specialty")
    .rlike("Audio")
  ).distinct().display()
