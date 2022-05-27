
from mrjob.job import MRJob
import re

class PartB1_Initial_Agregation(MRJob):
  

   def mapper(self,__,line):
       
        fields = line.split(",")
        try:
            if len(fields) == 7:   
                to_address = fields[2] 
                value = float(fields[3])
                yield(to_address,value)
        except:
            pass

   def combiner(self,address,total): 
        yield (address, sum(total))

   def reducer(self,address,total):
        yield (address, sum(total)) ##yield address and the total value

if __name__ == '__main__':

    PartB1_Initial_Agregation.run()
