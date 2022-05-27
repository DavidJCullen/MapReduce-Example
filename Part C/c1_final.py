from mrjob.job import MRJob

"""The below aggregates the total blocks mined by each miners"""

class PartC1(MRJob):

    def mapper (self,_,line) :
        
        try:
            fields = line.split(",")
            if len(fields) == 9:
                miner = fields[2]
                size = int(fields[4])
                yield (None,(miner, size))
        except:
            pass

    def combiner(self,key,value):

        sortedvalues = sorted ( value, reverse = True, key = lambda tup : tup [1])
        i = 0
        for a in sortedvalues:
            yield ("Top 10",a)
            i += 1
            if i >= 10:
                break

    def reducer(self,key,value):

        sortedvalues = sorted (value, reverse = True, key = lambda tup : tup [1])
        i = 0
        total = []
        for a in sortedvalues:
                yield (('Top 10 Miner:',a[0]))
                i+=1
                if i>=10:
                    break
if __name__=='__main__':
    PartC1.run()
