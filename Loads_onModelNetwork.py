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
import VisumPy.helpers as h

#connect to SQL DB in python
con = psql.connect(dbname = "GTFS", host = "localhost", port = 5432, user = "postgres", password = "sergt")
#create cursor to execute querys
cur = con.cursor()

#open visum
Visum = h.CreateVisum(15)

model = r"\\peach\Modeling\Base_Model_Repository\TIM 2.3.1 v2 Feb_2019\Completed Model Runs\2015\2015.ver"

#load model
Visum.LoadVersion(model)

#view max length for concatenated values
Visum.Net.AttValue("ConcatMaxLen")

#extend max length for contatenated values
Visum.Net.SetAttValue("ConcatMaxLen", 32768)

#get Line route attributes
lrid = h.GetMulti(Visum.Net.LineRoutes, "ID")
tsys = h.GetMulti(Visum.Net.LineRoutes, "TSysCode")
linename = h.GetMulti(Visum.Net.LineRoutes, "LineName")
lrname = h.GetMulti(Visum.Net.LineRoutes, "Name")
direction = h.GetMulti(Visum.Net.LineRoutes, "DirectionCode")
length = h.GetMulti(Visum.Net.LineRoutes, "Length")
StopsServed = h.GetMulti(Visum.Net.LineRoutes, "StopsServed")
NumVehJour = h.GetMulti(Visum.Net.LineRoutes, "Count:VehJourneys")
StopSequence = h.GetMulti(Visum.Net.LineRoutes, "Concatenate:StopPoints\No")
GTFSidSeq = h.GetMulti(Visum.Net.LineRoutes, "Concatenate:StopPoints\GTFS_STOP_ID")
NumLinks = h.GetMulti(Visum.Net.LineRoutes, "Count:Links")
LinkSequence = h.GetMulti(Visum.Net.LineRoutes, "Concatenate:Links\No")
FromNodeSeq = h.GetMulti(Visum.Net.LineRoutes, "Concatenate:Links\FromNodeNo")
ToNodeSeq = h.GetMulti(Visum.Net.LineRoutes, "Concatenate:Links\ToNodeNo")

#get stop point attributes
spid = h.GetMulti(Visum.Net.StopPoints, "No")
GTFSid = h.GetMulti(Visum.Net.StopPoints, "GTFS_STOP_ID")
spcode = h.GetMulti(Visum.Net.StopPoints, "Code")
spname = h.GetMulti(Visum.Net.StopPoints, "Name")
onlink = h.GetMulti(Visum.Net.StopPoints, "IsOnLink")
linkno = h.GetMulti(Visum.Net.StopPoints, "LinkNo")
fromnode = h.GetMulti(Visum.Net.StopPoints, "FromNodeNo")
tonode = h.GetMulti(Visum.Net.StopPoints, "Distinct:Links\ToNodeNo")
numlines = h.GetMulti(Visum.Net.StopPoints, "NumLines")
linenames = h.GetMulti(Visum.Net.StopPoints, "Distinct:LineRoutes\LineName")
lrid_served = h.GetMulti(Visum.Net.StopPoints, "Distinct:LineRoutes\ID")

#convert sequence lists to arrays
AGTFSidSeq = []
for i in xrange(0, len(GTFSidSeq)):
    AGTFSidSeq.append(numpy.array(GTFSidSeq[i].encode('ascii')))
    
AStopSequence = []
for i in xrange(0, len(StopSequence)):
    AStopSequence.append(numpy.array(StopSequence[i].encode('ascii')))
    
ALinkSequence = []
for i in xrange(0, len(LinkSequence)):
    ALinkSequence.append(numpy.array(LinkSequence[i].encode('ascii')))
    
AFromNodeSeq = []
for i in xrange(0, len(FromNodeSeq)):
    AFromNodeSeq.append(numpy.array(FromNodeSeq[i].encode('ascii')))
    
AToNodeSeq = []
for i in xrange(0, len(ToNodeSeq)):
    AToNodeSeq.append(numpy.array(ToNodeSeq[i].encode('ascii')))

#convert other unicode values to regular values
Rtsys = []
for i in xrange(0, len(tsys)):
        Rtsys.append(tsys[i].encode('ascii'))

Rlinename = []
for i in xrange(0, len(linename)):
        Rlinename.append(linename[i].encode('ascii'))

Rlrname = []
for i in xrange(0, len(lrname)):
        Rlrname.append(lrname[i].encode('ascii'))

Rdirection = []
for i in xrange(0, len(direction)):
        Rdirection.append(direction[i].encode('ascii'))
        
#convert floats to integers
Ilrid = []
for i in xrange(0, len(lrid)):
    Ilrid.append(int(lrid[i]))

IStopsServed = []
for i in xrange(0, len(StopsServed)):
    IStopsServed.append(int(StopsServed[i]))

INumVehJour = []
for i in xrange(0, len(NumVehJour)):
    INumVehJour.append(int(NumVehJour[i]))

INumLinks = []
for i in xrange(0, len(NumLinks)):
    INumLinks.append(int(NumLinks[i]))
    
Q_CreateLRIDTable = """
CREATE TABLE IF NOT EXISTS public.lineroutes
(
    lrid integer,
    tsys text,
    linename text,
    lrname text,
    direction text,
    length double precision,
    StopsServed integer,
    NumVehJour integer,
    StopSequence text[],
    GTFSidSeq text[],
    NumLinks integer,
    LinkSequence text[],
    FromNodeSeq text[],
    ToNodeSeq text[]
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;"""

cur.execute(Q_CreateLRIDTable)

########################this is where things get crazy - lots of testing and things that don't work#############################
str_rpl = "(%s)" % (",".join("%s" for _ in xrange(len(Ilrid))))
#cur.execute("""BEGIN TRANSACTION;""")
batch_size = 10000
for i in xrange(0, len(Ilrid), batch_size):
    j = i+batch_size
    arg_str = ','.join(str_rpl % tuple(map(str, x)) for x in Ilrid[i:j])
    print arg_str
    #Q_Insert = """INSERT INTO public.lineroutes VALUES {0}""".format(arg_str)
    #cur.execute(Q_Insert)
#cur.execute("""COMMIT;""")


tester = []
for i in xrange(0, 5):
        a = []
        a.append(Ilrid[i])
        a.append(Rtsys[i])
        a.append(Rlinename[i])
        a.append(Rlrname[i])
        a.append(Rdirection[i])
        a.append(length[i])
        a.append(IStopsServed[i])
        a.append(INumVehJour[i])
        a.append(AStopSequence[i])
        a.append(AGTFSidSeq[i])
        a.append(INumLinks[i])
        a.append(ALinkSequence[i])
        a.append(AFromNodeSeq[i])
        a.append(AToNodeSeq[i])
        tester.append(a)
        
for i in xrange(0,5):
    cur.execute(
        """
        INSERT INTO public.lineroutes
        VALUES {0};
        """.format(tester[i]))
cur.execute("""COMMIT;""")


#create data frames to insert
lr_df = pd.DataFrame(
    {'lrid': lrid, 
     'tsys' : tsys,
     'linename' : linename,
     'lrname' : lrname,
     'direction' : direction,
     'length' : length,
     'StopsServed ' : StopsServed,
     'NumVehJour' : NumVehJour,
     'StopSequence' : AStopSequence,
     'GTFSidSeq' : AGTFSidSeq,
     'NumLinks' : NumLinks,
     'LinkSequence' : ALinkSequence,
     'FromNodeSeq ' : AFromNodeSeq,
     'ToNodeSeq ' : AToNodeSeq
    })
    
lr_47 = lr_df[lr_df.linename == '47']
lr_47

#drop df into postgres db
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:sergt@localhost:5432/GTFS')
lr_47.to_sql('lr47', engine, chunksize = 10000)