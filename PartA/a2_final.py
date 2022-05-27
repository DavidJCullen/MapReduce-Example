from mrjob.job import MRJob
import re
import time
import statistics



class PartA(MRJob):

    def mapper(self,_,line):
        fields = line.split(',') # split by comma
        try:
            if len(fields) == 7: # len equal 7 or pass on exception.
                value = float(fields[3]) # value field
                date = time.gmtime(int(fields[6]))# timestamp field
                date_my = time.strftime("%m%y",(date)) # convert timestamp to gmtime
                yield ((date_my), (1, value)) # yield date as key and integer and value as value in the key value pair.
        except:
            pass

    def combiner(self,key,price):
        count = 0 # count counter
        total = 0 # total counter
        for p in price: # loop through value and store count and total in above counters.
            count+= p[0]
            total+= p[1]
        yield (key,(count,total)) # yield date count and total.


    def reducer(self,key,price):  #as above but for reducer.
        count = 0
        total = 0
        for p in price:
            count += p[0]
            total += p[1]

        yield (key, (total/count)) #yield date and average value.

if __name__=='__main__':
    PartA.run()
