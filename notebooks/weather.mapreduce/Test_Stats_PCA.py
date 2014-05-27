
#!/usr/bin/python

"""

collect the statistics for each station.

"""

import re,pickle,base64,zlib
from sys import stderr
import sys
import numpy as np




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
        self.increment_counter('MrJob Counters','mapper-all',1)
        elements=self.map_one(line)
        #yield(elements[0],elements[1:])
        yield(elements[3],elements[:])            

            
    def reducer(self, station, vectors):
        self.increment_counter('MrJob Counters','reducer',1)
        sumvec=np.zeros(730)
        matr=np.zeros((730,730))
        tt=0
        data={}
        for vector in vectors:
            length=len(vector)
            try:
                la1=float(vector[4])
                la2=float(vector[5])
                lo1=float(vector[6])
                lo2=float(vector[7])
            except Exception, e:
                continue
            number_defined=sum([e!='' for e in vector[16:]])
            if number_defined==730:
                strvec=vector[16:]
                numvec=[float(ss) for ss in strvec]
                tt=tt+1
                data[tt]=numvec
        
        if tt!=0:
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
            for n in range(0,730):
                ptvar+=D[n]
                if ptvar/ttvar>0.99:
                    break
            m=matr.reshape((1,730*730)).tolist()[0]
            yield(station,[n,tt,la1,la2,lo1,lo2]+meanlis+m)                            

if __name__ == '__main__':
    MRWeather.run()                            