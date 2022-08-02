
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import *
import os



import pyspark.sql.functions as F
from pyspark.sql.functions import col, when, count
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import lower, col

json_df  = spark.read.option("delimiter",',').json('json_directory_path')
parquet_df = spark.read.parquet('/mnt/Processes_files/')

json_df.display()

# To select all  columns except some specified:
df1_0 = json_df.select([c for c in json_df.columns if c not in {'objectId','Name', 'fullname', 'cityState'}])
df1_0.display()

# To save distinct values of each df column in azure storage:
try:
    for i in df1_0.columns:
        path = '/mnt/raw/saltanat/'+i+'.csv'
        new  = df1_0.select(col(i)).distinct()
        new.coalesce(1).write.format('csv').option("header", True).save(path)

except AttributeError:
    print('hello')
    pass

  
# To count distinct values from each df column: 
from pyspark.sql.functions import col, countDistinct
#df.agg(*(countDistinct(col(c)).alias(c) for c in df.columns))
l  = df1_0.agg(*(countDistinct(col(c)).alias(c) for c in df1_0.columns))
l.display()


# To select distinct values from specified columns:
df.select('Name','ProcedureName').distinct().display()


# To select only 1st row of df:
df.limit(1).display()

# To select 100 rows:
display(df.take(100))

# Drop duplicate values in df:
df= df.dropDuplicates(['ID', 'insuranceName', 'insuranceCompany'])


# Group by one column and count:
display(df.groupBy('insuranceName').count().sort(F.col('count').desc()).take(10))


# Rename column name:
df = df.withColumnRenamed("count",'NUM')


# Convert string values in the df rows in specific column to upper case letters:
df = df.select(upper(col('certifications')))
df.distinct().count()

# Split column values to multiple columns:
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col,substring,regexp_replace

split_col = pyspark.sql.functions.split(df['awards'], ', ')
df3 = df.select("awards",'ID',  split_col.getItem(0).alias('awards_clean'),split_col.getItem(1).alias('year'))   


# Replace column value if it starts with specific string:
from pyspark.sql.functions import col,when
nn = df3.withColumn("RATING", \
       when(col("RATING").startswith("("),None) \
          .otherwise(col("RATING"))) 

# To select all rows equal to the specified column value:
df.filter(col('Student_ID')=='1720036809').display()


# Drop column:
df = df.drop('REVIEW NUMBER')


import pyspark.sql.functions as F

df = df.filter(("RATING >= 4.1 and RATING <= 5.0"))
display(d)
    

print(df.agg(F.sum("count")).collect()[0][0])

# change the column type to integer/string:
db  = new.select(col("RATING").cast("int"))
df  = df.select(col("age").cast("string"))

# select max value in a column:
db.groupby().max('RATING').collect()[0].asDict()['max(RATING)']

# replace column values with None, if the column value equals to "//photos/silhouette-male_w90h120_v1.jpg":
from pyspark.sql.functions import col,when
nn = df.withColumn("expert_photo", \
       when(col("expert_photo")=="//photos/silhouette-male_w90h120_v1.jpg" ,None) \
          .otherwise(col("expert_photo")))


# Count not null values in column:
from pyspark.sql.functions import col
print(df.filter(col("ratingAndReviewNumber").isNotNull()).count())


# Get min value in the column:
df.groupby().min('age').collect()[0].asDict()['min(age)']

# Select the rows in df column with similar values:
display(df.filter(col("specialty").like("Audio")))


# Select distinct values in a column:
display(df.select('practiceLocationMainName').distinct())

# Select rows equal to the value:
df = df.filter(df.Level == '1')

#Join on ID:
df.join(AudioL,df.ID == AudioL.ID,"inner").display()
