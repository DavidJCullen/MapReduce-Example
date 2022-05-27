from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class Scams(MRJob):

    def mapper1(self, _, lines):
	
        try:
		
            fields = lines.split(",")
            if len(fields) == 7:
            # if '[' not in lines:
                to_address = fields[2]
                value = float(fields[3])
                yield (to_address, (1, value, 0))
			# csv file
            else:

                category = fields[2]
                status = fields[3]
                address = fields[4]
                yield (address, (status,category,1))
        except:
            pass

    def reducer1(self, key, values):
	#create empty containers for storing transactions and counts
        transaction = 0
        category = []
        status =  []
        count = 0
        for v in values:
            if v[2] == 0: 
                transaction += v[1]

            else:
                category.append(v[1])
                count  += 1
                status.append(v[0])

        if category and status and transaction > 0:
            for a,b in zip(category,status):
                yield ((a,b), (transaction))

    def mapper2(self,key,value):
        yield(key,value)

    def reducer2(self, key, value):
        yield(key,(sum(value)))

    #
    def steps(self):
        return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]



if __name__ == '__main__':
    Scams.run()
