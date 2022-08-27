from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

n = 3
dd.select(col('SpecialtyName'), row_number().over(window).alias('row_number')) \
  .where(col('row_number') <= n) \
  .filter()
  .display()
  
  
  https://sparkbyexamples.com/spark/spark-sql-window-functions/
