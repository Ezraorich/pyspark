from pyspark.sql.functions import lit


dbutils.widgets.text('p_data_source', '')
v_data_source = bdutils.widgets.get('p_data_source')

circuits_renamed_df = circuits_selected_df.withColumnrenamed('circuitsId', 'circuit_id')\
.withColumnRenamed('circuitref', circuit_ref)\
.withColumnRenamed('lat', 'latitude')\
.withColumnRenamed('lng', 'longitude')\
.withColumnRenamed('alt', 'altitude')\
.withColumn('data_source', lit(v_data_source))


v_result = bdutils.notebook.run('8.inget_qualifuing_file', 0, {"p_data_source": "Ergast API"})
dbutils.notebook.exit("Success")
