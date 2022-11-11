spark.conf.set('spark.databricks.optimizer.dynamicPartitionPruning', 'true')

from delta.tables import DeltaTable
if (spark._jsparkSession.catalog().tableExists('f1_processes.results')):
  deltaTable =DeltaTable.forPath(spark, '/mnt/formula1d/processes/results')
  deltaTable.alias('tgt').merge(
    results_final_df.alias('src'),
    'tgt.results_id = src.result_id AND tgt.race_id = src.race_id') \
  .whenMatchedUpdateAll()\
  .whenNotMatchedInsertAll()\
  .execute()
else:
  results_final_df.write.moode('overwrite').partitionBy('race_id').format('delta").saveAsTable("f1_processed.results")
                                                                          
                                                                          
### function:
merge_condition = 'tgt.results_id = src.result_id AND tgt.race_id = src.race_id'
merge_delta_data(results_final_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')                                                                         
                                                                          
def merge_delta_data (input_df, db_name, table_name, forlder_path, merge_condition, partition_column):
  spark.conf.set('spark.databricks.optimizer.dynamicPartitionPruning', 'true')

  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists(f'{db_name}.{table_name}')):
    deltaTable =DeltaTable.forPath(spark, f'{folder_path}/{table_name}')
    deltaTable.alias('tgt').merge(
      input_df.alias('src'),
      merge_condition) \
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
  else:
    input_df.write.moode('overwrite').partitionBy(partition_column).format('delta").saveAsTable(f"{db_name}.{table_name}")                                                                         
                                                                          
