access_key = "AKIAKKCO3BX4IOLFYYKL"
secret_key = "rFs12nXE+YpYoVta5D5vybGjBeRBHvRY4TlyoP0r"

#Mount bucket on databricks
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "forian-sample"
mount_name = "s3data"
dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s/" % mount_name))


dbutils.fs.ls('/mnt/s3data')

display(dbutils.fs.ls("/mnt/%s" % mount_name))

path = '/mnt/s3data/09_2020/manifest/09_2020_Sample_Manifest.xlsx'
manie = spark.read.format("com.crealytics.spark.excel").option("header", "true").load(path)
manie.display()



# write your own filename with the path to Azure Storage Explorer
filename = '/mnt/raw/saltanat/09_2020/manifest/09_2020_Sample_Manifest.xlsx'
manie.write.format("com.crealytics.spark.excel").option("header", "true").mode("overwrite").save(filename)


# to iteratively load the data to Azure Storage use for loop:
PATH  = '/mnt/s3data/folder_name/'


path_names = [] 
for i in dbutils.fs.ls(PATH):
    path_names.append(i[0])

    
# we are writing parquet files, but 'dbfs:/mnt/s3data/folder_name/Data Sample Manifest.xlsx'  is not the parquet file, therefore I wrote the if statement to exclude that file..
for i in range(len(path_names)):
    
    
    #print(path_names[i])
    #filename = "/mnt/claims/forian_12_12_22/"+
    if (path_names[i]!='dbfs:/mnt/s3data/folder_name/Data Sample Manifest.xlsx') and (path_names[i]!='dbfs:/mnt/s3data/folder_name/success.txt'):
        df = spark.read.parquet(path_names[i])
        filename = "/mnt/raw/saltanat/"+ path_names[i].split('/')[3]+'/'+path_names[i].split('/')[6]
        print(filename)
        df.write.mode('overwrite').parquet(filename)


