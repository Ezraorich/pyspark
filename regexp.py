from pyspark.sql import functions as f

graduation = institutionStudyYear_.withColumn("year",regexp_replace(col("institutionStudyYear"),"(\D)",""))\
.withColumn('start year', substring('year', 1,4))\
.withColumn('graduation year', substring('year', 5,4))

graduation.withColumn('D', f.when(f.col('year') == f.col('start year'), f.col('start year')).otherwise(f.col('graduation year'))).display()
