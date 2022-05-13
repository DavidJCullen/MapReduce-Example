
from mrjob.job import MRJob
import re

class PartB1_Initial_Agregation(MRJob):
    #define mapper

   def mapper(self,__,line):
       #split fields
        fields = line.split(",")
        try:
            if len(fields) == 7:    #if field lenght ==7 continue, otherwise pass.
                to_address = fields[2]
                value = float(fields[3])
                yield(to_address,value)
        except:
            pass

   def combiner(self,address,total): #use comobiner to speed up computation
        yield (address, sum(total))# yeild address and the total value

   def reducer(self,address,total):#send to reducer
        yield (address, sum(total)) ##yeild address and the total value

if __name__ == '__main__':

    PartB1_Initial_Agregation.run()
