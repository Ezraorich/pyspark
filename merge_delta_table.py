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
