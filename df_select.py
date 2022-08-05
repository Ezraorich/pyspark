NNN.filter(col('primarySpecialtyCode')!=('390200000X')).display()


physician.alias('l').join(education.alias('r'), on='physicianId').display()
