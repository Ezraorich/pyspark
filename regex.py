import pyspark.sql.functions as F
ndc10_regx532 = '^(\d{5})\-(\d{3})\-(\d{2})$'
ndc10_regx442 = '^(\d{4})\-(\d{4})\-(\d{2})$'
ndc10_regx541 = '^(\d{5})\-(\d{4})\-(\d{1})$'



ndc_10 = NDC_dirty.where(F.col('ndc').rlike(ndc10_regx532)|F.col('ndc').rlike(ndc10_regx442)|F.col('ndc').rlike(ndc10_regx541))\
.withColumnRenamed('str', 'str_10')\
.withColumnRenamed('ndc', 'ndc_10')\
.distinct()

ndc_10_ = ndc_10.withColumn('ndc11',F.regexp_replace(F.col('ndc_10'),ndc10_regx532,'$10$2$3'))\
.withColumn('ndc11',F.regexp_replace(F.col('ndc11'),ndc10_regx442,'0$1$2$3'))\
.withColumn('ndc11',F.regexp_replace(F.col('ndc11'),ndc10_regx541,'$1$20$3'))\
.distinct()
