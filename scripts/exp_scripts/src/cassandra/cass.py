import os,sys 
import pycassa 
import logging 
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from datetime import datetime as d 

__author__ = 'rahul'


logging.basicConfig(filename="example.log",level=logging.DEBUG,format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S')

pool = ConnectionPool('icepice')
col_fam = ColumnFamily(pool, 'info')

#Sample Insert operations 
#col_fam.insert('row_key',{'col_name' : 'value' })

'''Sample Get operations. 
Need to wrap the get operations in exception block for safe failing from
'key not found error'.
'''
try:
  for i in range(0,1000):
    s = d.now()
    col_fam.get(i)
    logging.info(str(i) +"=>"+ str((d.now() - s).microseconds))

except:
  logging.info("Error" + str(sys.exc_info()[0]))

