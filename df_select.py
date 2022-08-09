NNN.filter(col('primarySpecialtyCode')!=('390200000X')).display()


physician.alias('l').join(education.alias('r'), on='physicianId').display()

# selecting not NUll and only distinct values from 2 columns.
display(CList.select('Specialty', 'Idc').distinct().where(col("Specialty").isNotNull()).limit(100))
ss = CList.select('Specialty', 'Idc').distinct().where(col("Specialty").isNotNull())
ss.dropDuplicates(['primarySpecialty']).display()


# to see all column names:
ss.columns


# finding the length of string values in column:
S= df14.select('formattedDate').withColumn('length', 
                F.length(col('formattedDate')))


# selecting the column with length where is less than 5:
S.filter('length<5').display()

#show the columns containing some value, ex:.
df14.filter(col('formattedDate').contains('.')).display()

# Not equal to some value: filter:
new = df13.filter(col('practiceLocationMainName')!='Practice')


from pyspark.sql.functions import col
cms_cert =cms_certification.filter( (cms_certification.certificationName  == "American") | (cms_certification.certificationName  == "Board (Clinical Informatics)") |(cms_certification.certificationName  =='HHHAHXHBXHSB')|(cms_certification.certificationName=='Amejskksk, PA') )
cms_cert.count()

dfbio.subtract(dfdescription).display()

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when
ee = df17.withColumn('expert_photo', 
    when(df17.expert_photo.endswith('//photos.healthgrades.com/img/silhouettes/silhouette-male_w90h120_v1.jpg'),regexp_replace(df17.expert_photo,'//photos.healthgrades.com/img/silhouettes/silhouette-female_w120h160_v1.jpg',None)) \
   .when(df17.expert_photo.endswith('//photos.healthgrades.com/img/silhouettes/silhouette-female_w90h120_v1.jpg'),regexp_replace(df17.expert_photo,'//photos.healthgrades.com/img/silhouettes/silhouette-female_w90h120_v1.jpg',None)) \
   
   .otherwise(df17.expert_photo)) \

from pyspark.sql.functions import col,when
nn = hg_bio.withColumn("expert_photo", \
       when(col("expert_photo")=="//photos.health.com/img/silhouettes/silhouette-female_w90h120_v1.jpg" ,None)
       .when(col("expert_photo")=="//phot.com/img/silhouettes/silhouette-male_w90h120_v1.jpg" ,None)
       .when(col("expert_photo")=="//phots.com/img/silhouettes/silhouette-male_w120h160_v1.jpg" ,None) 
       .when(col("expert_photo")=="//photocom/img/silhouettes/silhouette-female_w120h160_v1.jpg" ,None)
       .when(col("expert_photo")=="//photos.com/img/silhouettes/silhouette-unknown_w90h120_v1.jpg" ,None)
       .when(col("expert_photo")=="//photos.com/img/silhouettes/silhouette-unknown_w120h160_v1.jpg" ,None) 
          .otherwise(col("expert_photo")))
   

