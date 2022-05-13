from mrjob.job import MRJob
import statistics
import time



"""
Create a bar plot showing the number of transactions occurring every month between the start and end of the dataset. Create a bar plot showing the average value of transactions in each month between the start and end of the dataset. Note: As the dataset spans multiple years and you are aggregating together all transactions in the same month, make sure to include the year in your analysis.
Note: Once the raw results have been processed within Hadoop/Spark you may create your bar plot in any software of your choice (excel, python, R, etc.)

"""
class PartA1(MRJob):#define the mapper.
	def mapper(self, _,line):
		fields = line.split(',') # split by comma
		try: # the transactions field has 7 fields. The below prevents malformed lines being inluded by requiring the lenght is equal to 7.
			if len(fields) == 7: # if lenght of line is 7 run the code otherwise pass on the exception (passes malformed lines)
				unix_timestamp = int(fields[6]) # access unix timestamp.
				date = time.strftime("%m%y", time.gmtime(unix_timestamp)) # convert unix to gmtime.
				yield ((date), 1) # yeild date as key and 1 as value.
		except:
			pass

	def combiner(self,key,counts): #combiner to add computational efficiency.
		yield(key, sum(counts)) # yeild date and and count.

	def reducer(self,key,counts):
		yield(key,sum(counts))# yield date and and count.

if __name__ == '__main__':
	PartA1.run()
