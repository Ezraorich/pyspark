# now we will find size of each folder in Azure Storage using Azure Databricks.


PATH  = '/mnt/saltanatdatalakegen2/extract_date=2021-08-13/'

path_names = [] 
for i in dbutils.fs.ls(PATH):
    path_names.append(i[0])
    
    



# we dont want to get the size of specific file called "One Month Sample Data Manifest.xlsx", that's why iterate through all files, except that file using if condition.
import os
for i in range(len(path_names)):
    if path_names[i]!='dbfs:/mnt/saltanatdatalakegen2/extract_date=2021-08-13/extract_date=2021-08-13/One Month Sample Data Manifest.xlsx':
        print(path_names[i])
        
        ## to get the dirertory size, the foldername should be started with /dbfs/
        folder = '/dbfs/'+path_names[i].split('/')[1]+'/'+path_names[i].split('/')[2] +'/'+path_names[i].split('/')[3]+'/' +path_names[i].split('/')[4]+'/'+path_names[i].split('/')[5]
    
    


 
        size = 0  
        for path, dirs, files in os.walk(folder):
            for f in files:
                fp = os.path.join(path, f)
                size += os.path.getsize(fp)
 

        print("Folder size: " + str(size),'bytes; ', size/1024/1024, 'MegaBytes;', size/1024/1024/1024, 'Gigabytes.', '\n')
