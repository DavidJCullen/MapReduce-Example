from mrjob.job import MRJob
from mrjob.step import MRStep
import json


"""

Popular Scams: Utilising the provided scam dataset, what is the most lucrative form of scam?
Does this correlate with certainly known scams going offline/inactive? For the correlation, you could produce
the count of how many scams for each category are active/inactive/offline/online/etc and try to correlate it with volume
(valu python preprocessing.py scams.jsone) to make conclusions on whether state plays a factor in making some scams more lucrative.
Therefore, getting the volume and state of each scam, you can make a conclusion whether the most
lucrative ones are ones that are online or offline or active or inactive. So for that purpose,
 you need to just produce a table with SCAM TYPE, STATE, VOLUME which would be enough (15%).

"""

class Scams(MRJob):

    def mapper1(self, _, lines):
        try:
			#transactions
            fields = lines.split(",")
            if len(fields) == 7:
            # if '[' not in lines:
                to_address = fields[2]
                value = float(fields[3])
                yield (to_address, (1, value, 0))# yeild to address, 1 for counter later on. value ad 0 as yeild value indentifier.
			# csv file
            else:

                category = fields[2]
                status = fields[3]
                address = fields[4]
                yield (address, (status,category,1))
        except:
            pass

    def reducer1(self, key, values):
		#create empty containers for stroring transactions and counts
        transaction = 0
        category = []
        status =  []
        count = 0
        for v in values:
            if v[2] == 0: #container to check if in transaction yeild.
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
