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

