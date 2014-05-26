#!/usr/bin/python
"""
count the number of measurements of each type
"""
import sys
import numpy as np
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

class MRWeather(MRJob):

    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            elements=line.split(',')
            number_defined=sum([e!='' for e in elements[3:]])
            if number_defined==365:
                if elements[0]!='station':
                    yield (elements[0],1)

        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)

            

    def reducer(self, word, counts):
        self.increment_counter('MrJob Counters','reducer',1)
        yield (word,sum(counts))
        #l_counts=[c for c in counts]  # extract list from iterator
        #S=sum(l_counts)
        #logfile.write('reducer '+word+' ['+','.join([str(c) for c in l_counts])+']='+str(S)+'\n')
        #yield (word, S)

if __name__ == '__main__':
    MRWeather.run()