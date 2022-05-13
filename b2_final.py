from mrjob.job import MRJob

class PartB_2(MRJob):

    def mapper(self,_,line):
        try:
            if (len (line.split ('\t') )== 2) : # aggregated file (output of job B1)
                join_key = fields [0][1:-1] #address
                join_val = float (fields [1] ) # value aggregated
                yield (join_key, (join_val, 1))

            elif (len (line.split (',')) == 5): # contracts data
                join_key = fields[0] # to_address
                yield (join_key, (None, 2))

        except:
            pass


    def reducer(self, address, values):
        amount = 0
        for value in values:
            if value[1]==1:
                amount=value[0]

        if amount > 0:#if the value is greater than 0 (aggregate value), store the value of the transaction in the amount variable
            yield (address, amount)

if __name__=='__main__':
    PartB_2.run()
