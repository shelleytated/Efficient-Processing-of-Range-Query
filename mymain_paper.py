import my_partitioning as Partioning
import my_rangequery as RangeQuery

import datetime


# host = ["postgres1.c1lmbamulyk4.us-east-2.rds.amazonaws.com", "postgres2.c1lmbamulyk4.us-east-2.rds.amazonaws.com",
#             "postgres3.c1lmbamulyk4.us-east-2.rds.amazonaws.com", "postgres4.c1lmbamulyk4.us-east-2.rds.amazonaws.com",
#             "postgres5.c1lmbamulyk4.us-east-2.rds.amazondatabase-2.cpnta8q4u7uo.us-west-2.rds.amazonaws.comaws.com",]
host= ["database-1.cmlmc1funixs.us-west-1.rds.amazonaws.com",
            'database-2.cpnta8q4u7uo.us-west-2.rds.amazonaws.com']
# rangequerydb-3.cyw98q1oeofi.us-east-2.rds.amazonaws.com
# rangequerydb-4.csdc6a3y1psp.us-east-1.rds.amazonaws.com
# dbname = ["dds1", "dds2","dds3","dds4","dds5"]
dbname = ["database1", "database2"]
port=[5432,5432]
print("Creating Databases in all the servers")
Partioning.createDB()

print("Getting connection from all the servers")
con = Partioning.getOpenConnection()
conAWS = []
for i in range(2):
    conAWS.append(Partioning.getOpenConnectionAWS(host[i], port[i], dbname[i]))

print("Creating and Loading the ratings table")
Partioning.loadRatings('ratings','data/datadat.dat',con)

st = datetime.datetime.now()
print("Doing Range Partitioning")
Partioning.rangePartition('ratings',2,con, conAWS)
en = datetime.datetime.now()
print("Time taken for range partitioning ", en-st)


en=st=0
st = datetime.datetime.now()
print("Performing normal Range Query")
RangeQuery.RangeQuery('ta','za',3,con, conAWS)
en = datetime.datetime.now()
print("Time taken for normal range query ",en-st)

st=en=0
st = datetime.datetime.now()
print("Performing Fast Range Query")
RangeQuery.FastRangeQuery('ta','za',3,con, conAWS)
en = datetime.datetime.now()
print("Time taken for fast range query ",en-st)

# print("Deleting all Tables")
#Partioning.deleteTables('all',con, conAWS)