from mrjob.job import MRJob
import statistics
import time


class PartA1(MRJob):
	def mapper(self, _,line):
		fields = line.split(',') # split by comma
		try: 
			if len(fields) == 7: # if length of line is 7 run the code, otherwise pass on the exception (passes malformed lines)
				unix_timestamp = int(fields[6]) # access unix timestamp.
				date = time.strftime("%m%y", time.gmtime(unix_timestamp)) # convert unix to gmtime.
				yield ((date), 1) # yield date as key and 1 as value.
		except:
			pass

	def combiner(self,key,counts): #combiner to add computational efficiency.
		yield(key, sum(counts)) # yeild date and and count.

	def reducer(self,key,counts):
		yield(key,sum(counts))# yield date and and count.

if __name__ == '__main__':
	PartA1.run()
