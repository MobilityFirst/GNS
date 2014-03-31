import os 
import pycassa 
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

__author__ = 'rahul'


pool = ConnectionPool('keyspace_name')
col_fam = ColumnFamily(pool, 'table_name')

#Sample Insert operations 
col_fam.insert('row_key',{'col_name' : 'value' })

'''Sample Get operations . 
Need to wrap the get operations in exception block for safe failing from
'key not found error' .
'''
col_fam.get('row_key')

