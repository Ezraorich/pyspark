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
        
        
    
    #data = [char for char in my_list if char.islower()]
    #return ' '.join(data)
