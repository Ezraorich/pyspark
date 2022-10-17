#   Method1  INCREMENTAL LOAD


%sql 
--DROP TABLE f1_processes.results

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')


def re_arrange_partition_column(input_df, partition_column):
  column_list =  []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

output_df  = re_arrange_partition_column(results_final_df, 'race_id')


def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition(input_df, partition_column)
  spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode('overwrite').insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode('overwrite').partitionBy(partition_column).format('parquet').saveAsTable(f"{db_name}.{table_name}")
  
overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')  
  
  
  
  

## 'race_id' should be the last column, because it is used for partition:
results_final_df  = results_final_df.select('results_id', 'driver_id', 'constructor_id', 'number', 'grid', 'position_text', 'position_order', 
                                            'file_date',
                                            'ingestion_date',
                                           'race_id') 



if (spark._jsparkSession.catalog().tableExists('f1_processes.results')):
  results_final_df.write.mode('overwrite').insertInto('f1_processes.results')
else:
  results_final_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable(' f1_processed.results')
  
results_final_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

results_final_df.write.mode('overwrite').insertInto('f1_processes.results')

#   Method2  INCREMENTAL LOAD


for race_id_list in results_final_df.select('race_id').distinct().collect():
  if (spark._jsparkSession.catalog().tableExists('f1processed.results'):
      spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})
  


results_final_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processes.results')


