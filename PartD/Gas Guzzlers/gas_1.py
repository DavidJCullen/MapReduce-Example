from mrjob.job import MRJob
import time

class Gas_Part_D_Gas(MRJob):

    def mapper (self,_,line) :
        try:
            fields = line.split(",")
            if len(fields) == 9:
                gas_used = float(fields[6])
                blocks = float(fields [0])
                difficulty = float (fields [3])
                unix_timestamp = int(fields[7])
                date =  time.gmtime(unix_timestamp)
                time_stmp = time.strftime("%m%y", date)
                yield (blocks, (1, gas_used, difficulty, time_stmp))
            #contracts
            elif len(fields) == 5:
                #fields = line.split(',')
                blocks = float(fields[3])
                yield (blocks, (2, "B"))

        except:
            pass

    def reducer (self, key, values):

        list_values = [] # gas, difficulty, date (month and year)
        check = 0 #
        #try:
        for value in values:
            if value[0] == 1: #create a map that equals 1 to append the list values.
                list_values.append((value[1], value[2], value[3])) # append the elements of the first reducer to the list
            elif value[0] == 2: #create a map that equals 2 amd add check value.
                check = 1

        if check == 1 and list_values :
            for each in list_values:
                yield (each[2], (each[0], each[1]))



if __name__=='__main__':
    Gas_Part_D_Gas.run()
