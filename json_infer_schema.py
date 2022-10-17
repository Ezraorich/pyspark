#from the course Real World Project on Formula1 Racing for Data Engineers using Azure Databricks, Delta Lake, Azure Data Factory [DP203]


from pyspark.sql.functions import col, concat, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, Datetype


name_schema = StructType(fields = [StructField('forename', StringType(), True),
                                  StructField('surname', StringType(), True)])

drivers_schema = StructType(fields = [StructField('driverId', IntegerType(), False),
                                     StructField('driverRef', StringType(), True),
                                     StructField('number', IntegerType(), True),
                                     StructField('code', StringType(), True), 
                                     StructField('name', name_schema),
                                     StructField('dob', DateType(), True),
                                     StructField('nationality', StringType(), True),
                                     StructField('url', StringType(), True)
                                      
                                     ])

drivers_df = spark.read \
 .schema(drivers_schema)\
 .json('/mnt/formula1/raw/drivers.json')

lap_times_df = spark.read \
 .schema(lap_times_schema) \
 .csv('/mnt/formula1/raw/lap_times/lap_times_split*.csv')

drivers_with_column_df = drivers_df.withColumnRenamed('driverId', 'driver_id')\
                                   .withColumnRenamed('driverRef', 'driver_ref')\
                                   .withColumn('ingestion_date', current_timestamp())\
                                   .withColumn('name', concat(col('name.forename'), lit(' '), col('col.name.surname')))


drivers_with_column_df.write.mode('overwrite').parquet('/mnt/formula1/qualifying')




## Method 2:
from pyspark.sql.functions import *

dataframe = spark.read.format('delta').load('/mnt/path/')

hospital = dataframe.select(col('HOSPITALAFFILIATION_LIST').alias('organization_name'), col('ID').alias('id_number'))

hos = hospital.select(hospital.id_number, posexplode(hospital.organization_name))

hos_split = hos.withColumn("organization_name", hos.col.getItem("EXPERT_HOSPITAL_AFFILIATION"))\
.withColumn("org_address1", hos.col.getItem("EXPERT_HOSP_AFF_STREETADDRESS"))\
.withColumn("EXPERT_HOSP_AFF_ADDRESSCOUNTRY", hos.col.getItem('EXPERT_HOSP_AFF_ADDRESSCOUNTRY'))\
.withColumn("org_city", hos.col.getItem("EXPERT_HOSP_AFF_CITY"))\
.withColumn("org_state", hos.col.getItem("EXPERT_HOSP_AFF_STATE"))\
.withColumn("EXPERT_HOSP_AFF_POSTALCODE", hos.col.getItem("EXPERT_HOSP_AFF_POSTALCODE"))

