
%run "../includes/configuration"

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
race_results_df.createTempView("v_race_results")

race_resultsdf.createOrReplaceTempView("v_race_results ")

%sql 
SELECT COUNT(1)
FROM v_race_results
WHERE race_year = 2020 

%sql

SELECT * FROM v_race_results 
