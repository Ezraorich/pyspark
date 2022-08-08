NNN.filter(col('primarySpecialtyCode')!=('390200000X')).display()


physician.alias('l').join(education.alias('r'), on='physicianId').display()

# selecting not NUll and only distinct values from 2 columns.
display(CList.select('Specialty', 'Idc').distinct().where(col("Specialty").isNotNull()).limit(100))
ss = CList.select('Specialty', 'Idc').distinct().where(col("Specialty").isNotNull())
ss.dropDuplicates(['primarySpecialty']).display()
