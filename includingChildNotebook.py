%run "../includes/configuration"


raw_folder_path = '/mnt/formula1/raw'  (notebook saved in configuration contains this info about path)

circuits_df = spark.read \
.option('header', True)\
.schema(circuits_schema)\
.csv(f{raw_folder_path}/circuits.csv)
