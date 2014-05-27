#!/usr/bin/python
"""
collect the statistics for each station.
"""
import re,pickle,base64,zlib
from sys import stderr
import sys
import pandas as pd
import numpy as np
import sklearn as sk

sys.path.append('/usr/lib/python2.6/dist-packages') # a hack because anaconda made mrjob unreachable
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
        #yield(elements[0],elements[1:])
        yield('sum',elements[1:])
            
    def check_integrity(self,meas,year,length):
        year=int(year)
        if year<1000 or year > 2014: return False
        if meas=='': return False
        if length != 367: return False
        return True
            
    def reducer(self, station, vectors):
        sumvec=np.zeros(365)
        matr=np.zeros((365,365))
        tt=0
        data={}
        for vector in vectors:
            meas=vector[0]
            year=vector[1]
            length=len(vector)
            number_defined=sum([e!='' for e in vector[2:]])
            if self.check_integrity(meas,year,length)==True and number_defined==365:
                if station=='sum':
                    strvec=vector[2:]
                    numvec=[int(ss) for ss in strvec]
                    tt=tt+1
                    data[tt]=numvec
        if station=='sum':
            for n in range(1,tt+1):
                sumvec=sumvec+np.array(data[n])
            sumlis=sumvec.tolist()
            meanlis=[float(n)/tt for n in sumlis]
            for n in range(1,tt+1):
                subtarr=np.array(data[n])-np.array(meanlis)
                matr+=np.outer(subtarr,subtarr)
            matr=np.divide(matr,tt)
            U,D,V=np.linalg.svd(matr)
            varexp=[]
            ptvar=0
            ttvar=sum(D)
            for n in range(0,365):
                ptvar+=D[n]
                if ptvar/ttvar>0.99:
                    break
            yield('PCA',n)
                              
if __name__ == '__main__':
    MRWeather.run()