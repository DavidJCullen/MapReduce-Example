from mrjob.job import MRJob
import re
import time
class Gas2(MRJob):
    def mapper(self,__,line):
        fields = line.split(",")
        tab = line.split(",")
        try:
            if len(fields) == 7:
                to_address = fields[2]
                gas_price = fields[3]
                unix_timestamp = int(fields[6])
                date =  time.gmtime(unix_timestamp)
                time_stmp = time.strftime("%m%y", date)
                yield(to_address,(1, gas_price,time_stmp))

            elif len(tab) == 2:
                join_key = tab[0]
                yield (join_key, (2,None))
        except:
            pass
    def reducer(self,key,values):
        list_values = []
        check = 0
        for value in values:
            if value[0] == 1:
                list_values.append((value[1],value[2]))
            elif value[0] == 2:
                check += 1
        if check ==1:
            for each in list_values:
                yield (each[1],(key,each[0]))

if __name__ == '__main__':

    Gas2.run()
