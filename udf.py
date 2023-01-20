import re

@udf(returnType=StringType()) 
def getBrandName(str1):
    a = []
    my_list = str1.split(' ')
    for i in range(len(my_list)):
         if '[' in my_list[i]:
                d = len(my_list)
                a.append(my_list[i:d])
                flat_list = [item for sublist in a for item in sublist]
                data = ' '.join(flat_list)
       
    if len(a)==0:
        a = None
            

    return data
        
      
    
    
@udf(returnType=StringType()) 
def getStrength(str1):
    str1 = re.sub("\[.*?\]","", str1)
    str1 = str1.replace('. ', '')
    my_list = str1.split(' ')
    data = []
    list_length = len(my_list)
    for i in range(len(my_list)):
        a = re.sub('[^\d.,]' , '', my_list[i])
        if (a!='') and (i!=list_length):
            
            data.append(my_list[i])
            data.append(my_list[i+1]) 
            if len(data)>2:
                x = len(data)
                strength = data[x-2:x+1]
            else:
                strength = data
            strength_with_MG = ' '.join(strength)
            if len(data) == 0:
                strength_with_MG = None            
    return strength_with_MG
