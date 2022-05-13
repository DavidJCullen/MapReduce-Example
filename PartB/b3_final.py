from mrjob.job import MRJob

class PartB(MRJob):

    def mapper(self,_,line):

        try:

            fields = line.split('\t')
            if len(fields) == 2:
                address = fields[0]
                aggregate = float(fields [1])
                yield (None, (address , aggregate))

        except:
            pass

    def combiner(self,key,value):

        sortedvalues = sorted ( value, reverse = True, key = lambda tup : tup [1])
        i = 0
        for a in sortedvalues:
            yield ("Top 10 Value",a)
            i += 1
            if i >= 10:
                break

    def reducer(self,key,value):

        sortedvalues = sorted ( value, reverse = True, key = lambda tup : tup [1])
        i = 0
        for a in sortedvalues:
            yield ((a[0], a[1]))
            i+=1
            if i>=10:
                break

if __name__=='__main__':
    PartB.run()
