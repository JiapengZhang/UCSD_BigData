#!/usr/bin/python
"""
collect the statistics for each station.
"""
import re,pickle,base64,zlib
from sys import stderr
import numpy as np

from mrjob.job import MRJob
from mrjob.protocol import *

import traceback
from functools import wraps
from sys import stderr

class MRWeather(MRJob):

    def map_one(self,line):
        return line.split(',')
    
    def mapper(self, _, line):
        elements=self.map_one(line)
        yield(elements[0],elements[1:])
        yield('sum',elements[1:])
            
    def check_integrity(self,meas,year,length):
        year=int(year)
        if year<1000 or year > 2014: return False
        if meas=='': return False
        if length != 367: return False
        return True
            
    def reducer(self, station, vectors):
        sumvec=np.zeros(365)
        tt=0
        for vector in vectors:
            meas=vector[0]
            year=vector[1]
            length=len(vector)
            number_defined=sum([e!='' for e in vector[2:]])
            if self.check_integrity(meas,year,length)==True and number_defined==365:
                if station=='sum':
                    strvec=vector[2:]
                    numvec=[int(ss) for ss in strvec]
                    sumvec=sumvec+np.array(numvec)
                    tt=tt+1
                #else:
                    #yield(station,vector)
        #sumvec=np.divide(sumvec,tt)
        if station=='sum':
            sumlis=sumvec.tolist()
            meanlis=[n/tt for n in sumlis]
            yield('sum',meanlis)
                              
if __name__ == '__main__':
    MRWeather.run()