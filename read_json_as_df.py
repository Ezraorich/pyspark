
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import *
import os


json_df  = spark.read.option("delimiter",',').json('json_directory_path')


json_df.display()
