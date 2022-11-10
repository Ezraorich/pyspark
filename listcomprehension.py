val =   ['ATTENDING_PROVIDER_CODE', 'RENDERING_PROVIDER_CODE']
#val  = ['ATTENDING_PROVIDER_CODE', 'RENDERING_PROVIDER_CODE','PAY_TO_PROVIDER_CODE', 'FACILITY_PROVIDER_CODE', 'REFERRING_PROVIDER_CODE', 'PCP_PROVIDER_CODE', 'LAB_PROVIDER_CODE']
full_list = []
for i in val:
    a = purple_diagnosis.select(col(i)).distinct().collect()
    newlist = [x[0] for x in a]
    #for j in range(len(a)):
        #full_list.append(a[j][0])
    full_list = [x for x in newlist]
        #full_list.append(newlist)
        
len(set(full_list))
