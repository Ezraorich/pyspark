val =   ['ATTENDING_PROVIDER_NPI_NBR', 'RENDERING_PROVIDER_NPI_NBR']
#val  = ['ATTENDING_PROVIDER_NPI_NBR', 'RENDERING_PROVIDER_NPI_NBR','PAY_TO_PROVIDER_NPI_NBR', 'FACILITY_PROVIDER_NPI_NBR', 'REFERRING_PROVIDER_NPI_NBR', 'PCP_PROVIDER_NPI_NBR', 'LAB_PROVIDER_NPI_NBR']
full_list = []
for i in val:
    a = purple_diagnosis_CD.select(col(i)).distinct().collect()
    newlist = [x[0] for x in a]
    #for j in range(len(a)):
        #full_list.append(a[j][0])
    full_list = [x for x in newlist]
        #full_list.append(newlist)
        
len(set(full_list))
