from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

from pyspark.sql.window import Window

ror = LOCATIONS_TABLE_RECOGNITIONINFO.withColumn("row_num", row_number().over(Window.partitionBy("RECOGNITIONCATEGORYNAME").orderBy("RECOGNITIONCATEGORYNAME")))
ror.filter(ror.row_num<4  ).display()
  
  
https://sparkbyexamples.com/spark/spark-sql-window-functions/
https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-functions-windows.html
