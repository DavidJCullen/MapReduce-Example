import pandas as pd
import json

f = open('scams.json')
line = json.load(f) # load json object.
object_field = line["result"] # result field of json object.

id =[]
category = []
addresses = []
status = []

for a in object_field:
    access = object_field[a]

    for addr in access["addresses"]:

        addresses.append(addr)
        category.append (access["category"])
        status.append(access["status"])
        id.append(a)

# #create new df
df = pd.DataFrame(list(zip(id,category,status,addresses)),columns = ['id','category','status','addresses'])

df.to_csv('Scams.csv',sep = ',',header =False)
