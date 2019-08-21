#this is script 3 - it fills in a table with link loads

import numpy
import os
import csv
import scipy
import pandas as pd
import psycopg2 as psql 
import sys
import itertools
import numpy
import time

#connect to SQL DB in python
con = psql.connect(dbname = "GTFS", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

cur.execute("""
    SELECT *
    FROM linkseq_cleanloads
    ORDER BY lrid, lrseq
    """)
loads = cur.fetchall()

#convert tupples to lists that can be changed
loads_list = []
for row in loads:
    list_row = []
    for item in row:
        list_row.append(item)
    loads_list.append(list_row)
    
#if load for first link in sequence is none, change to 0
counter = 0
for i in xrange(0, len(loads_list)):
    if loads_list[i][7] == 1:
        if loads_list[i][9] is None:
            loads_list[i][9] = 0
            
#testing
#test that it got all of them
counter = 0
for i in xrange(0, len(loads_list)):
    if loads_list[i][7] == 1:
        if loads_list[i][9] is None:
            counter+=1
print counter
#testing
#make sure there are not other weird first link values
firsts = []
for i in xrange(0, len(loads_list)):
    if int(loads_list[i][7]) == 1:
        firsts.append(loads_list[i][9])
        
#drop values down in list
holder = 0
for i in xrange(0, len(loads_list)):
    if int(loads_list[i][7]) == 1:
        holder = loads_list[i][9]
    else:
        if loads_list[i][9] is None:
            loads_list[i][9] = holder
        else:
            holder = loads_list[i][9]
            
df = pd.DataFrame(loads_list)
df.columns=['lrid',
                  'tsys',
                  'linename',
                  'direction',
                  'stopsserved',
                  'numvehjour',
                  'fromto',
                  'lrseq',
                  'count',
                  'load_portion_avg']
                  
#add to sql db
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:sergt@localhost:5432/GTFS')
df.to_sql('loaded_links', engine, chunksize = 10000)

