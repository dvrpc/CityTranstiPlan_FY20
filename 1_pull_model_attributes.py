#this is script 1 - use this to pull data from the model to put into postgres db

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
print Visum.Net.AttValue("ConcatMaxLen")

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

#split into arrays
AGTFSidSeq = []
for i in xrange(0, len(GTFSidSeq)):
    AGTFSidSeq.append(GTFSidSeq[i].split(','))
    
AStopSequence = []
for i in xrange(0, len(StopSequence)):
    AStopSequence.append(StopSequence[i].split(','))
    
ALinkSequence = []
for i in xrange(0, len(LinkSequence)):
    ALinkSequence.append(LinkSequence[i].split(','))
    
AFromNodeSeq = []
for i in xrange(0, len(FromNodeSeq)):
    AFromNodeSeq.append(FromNodeSeq[i].split(','))
    
AToNodeSeq = []
for i in xrange(0, len(ToNodeSeq)):
    AToNodeSeq.append(ToNodeSeq[i].split(','))
        
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

#create table for line routes
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

tester = []
for i in xrange(0, len(Ilrid)):
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
        
for i in xrange(0, len(tester)):
    cur.execute(cur.mogrify("INSERT INTO public.lineroutes VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tester[i]))
con.commit()



###REPEAT FOR STOP POINTS###

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


#replace null values with 0
for i in xrange(0, len(GTFSid)):
    if GTFSid[i] == None:
        GTFSid[i] = 0
        
for i in xrange(0, len(linkno)):
    if linkno[i] == None:
        linkno[i] = 0
        
for i in xrange(0, len(fromnode)):
    if fromnode[i] == None:
        fromnode[i] = 0        

#split into arrays
Alinenames = []
for i in xrange(0, len(linenames)):
    Alinenames.append(linenames[i].split(','))
    
Alrid_served = []
for i in xrange(0, len(lrid_served)):
    Alrid_served.append(lrid_served[i].split(','))
    
Atonode = []
for i in xrange(0, len(tonode)):
    Atonode.append(tonode[i].split(','))
    
#convert floats to integers
Ispid = []
for i in xrange(0, len(spid)):
    Ispid.append(int(spid[i]))

IGTFSid = []
for i in xrange(0, len(GTFSid)):
    IGTFSid.append(int(GTFSid[i]))

Ionlink= []
for i in xrange(0, len(onlink)):
    Ionlink.append(int(onlink[i]))

Ilinkno = []
for i in xrange(0, len(linkno)):
    Ilinkno.append(int(linkno[i]))
    
Ifromnode = []
for i in xrange(0, len(fromnode)):
    Ifromnode.append(int(fromnode[i]))
    
#Itonode = []
#for i in xrange(0, len(tonode)):
#    Itonode.append(int(tonode[i]))
    
Inumlines = []
for i in xrange(0, len(numlines)):
    Inumlines.append(int(numlines[i]))


#create table for line routes
Q_CreateSPTable = """
CREATE TABLE IF NOT EXISTS public.stoppoints
(
    spid integer,
    GTFSid integer,
    spcode text,
    spname text,
    onlink text,
    linkno integer,
    fromonode integer,
    tonode integer,
    numlines integer,
    linenames text[],
    lrid_served text[]
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;
COMMIT;"""

cur.execute(Q_CreateSPTable)

tester = []
for i in xrange(0, len(Ispid)):
        a = []
        a.append(Ispid[i])
        a.append(IGTFSid[i])
        a.append(spcode[i])
        a.append(spname[i])
        a.append(Ionlink[i])
        a.append(Ilinkno[i])
        a.append(Ifromnode[i])
        a.append(tonode[i])
        a.append(Inumlines[i])
        a.append(Alinenames[i])
        a.append(Alrid_served[i])
        tester.append(a)
        
for i in xrange(0, len(tester)):
    cur.execute(cur.mogrify("INSERT INTO public.stoppoints VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", tester[i]))
con.commit()



