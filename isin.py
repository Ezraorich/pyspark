def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct()\
                        .collect()
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list



race_year_list = df_column_to_list(race_results_df, 'race_year')
race_results_df  =spark.read.parquet(f"{presentation_folder_path}/race_results")\
.filter(col("race_year").isin(race_year_list))
