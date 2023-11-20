%python
import pyspark.sql.functions as F
import pyspark.sql.types as T
from fuzzywuzzy import fuzz

# CALCULATE SIMILARITY SCORE
# FUZZY MATCHING
def matchstring_tsr(s1,s2):
    return fuzz.token_set_ratio(s1,s2)
def matchstring_r(s1,s2):
    return fuzz.ratio(s1,s2)
tsrUDF = F.udf(matchstring_tsr,T.IntegerType())
rUDF = F.udf(matchstring_r,T.IntegerType())
address_mapping_fuzzy = cross_joins_inst.withColumn('LN_RT_ADDRESS', rUDF(F.col('ORG_NAME_LN'),F.col('INST_NAME_RYTE'))).distinct()
