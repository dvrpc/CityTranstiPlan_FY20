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

#grab existing loads table from postgres
cur.execute("""
SELECT *
FROM regionalrail_loads
ORDER BY linename, direction, sequ
""")
rrloads = cur.fetchall()

#convert to list to work with
rrloads_list = []
for row in rrloads:
    list_row = []
    for item in row:
        list_row.append(item)
    rrloads_list.append(list_row)

#create from/to node list associated based on stopids (since they are the node numbers for RR)
#fill in zeros where needed to keep lists lined up
fn = []
tn = []
for i in xrange(0, len(rrloads_list)-1):
    #if i < len(rrloads_list):
    j = i+1
    if rrloads_list[i][0] == rrloads_list[j][0]:
        if rrloads_list[i][3] == rrloads_list[j][3]:
            if int(rrloads_list[i][4]) < int(rrloads_list[j][4]):
                fn.append(int(rrloads_list[i][8]))
                tn.append(int(rrloads_list[j][8]))
        else:
            fn.append(0)
            tn.append(0)
    else:
        fn.append(0)
        tn.append(0)
#top it off
fn.append(0)
tn.append(0)

#combine from node and to node lists into fromto for joining with links (from line routes)
fromto = []
for i in xrange(0, len(fn)):
    fromto.append(int(str(fn[i])+str(tn[i])))

#check - how many zeros are there; they should only be at the end of each direciton for each line (26 total)
counter = 0
for i in xrange(0, len(fromto)):
    if fromto[i] == 0:
        counter+=1
print counter

#convert lists to dataframe
df = pd.DataFrame(rrloads_list)
df.columns=[
    'linename',
    'tsys',
    'stopname',
    'direction',
    'sequ',
    'boards',
    'leaves',
    'loads',
    'spid'
]

#add new column to dataframe
df['fromto'] = fromto

#add to sql db
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:sergt@localhost:5432/GTFS')
df.to_sql('regionalrail_loads_fromto', engine, chunksize = 10000)