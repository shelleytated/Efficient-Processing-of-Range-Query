import psycopg2
import sys

PRIME = 2
DATABASE_NAME = 'dds'
# host = ["postgres1.c1lmbamulyk4.us-east-2.rds.amazonaws.com", "postgres2.c1lmbamulyk4.us-east-2.rds.amazonaws.com",
#             "postgres3.c1lmbamulyk4.us-east-2.rds.amazonaws.com", "postgres4.c1lmbamulyk4.us-east-2.rds.amazonaws.com",
#             "postgres5.c1lmbamulyk4.us-east-2.rds.amazonaws.com"]
# dbname = ["dds1", "dds2","dds3","dds4","dds5"]
# alias_host = ["PostgreSQL AWS1", "PostgreSQL AWS2", "PostgreSQL AWS3", "PostgreSQL AWS4", "PostgreSQL AWS5"]
host= ["database-1.cmlmc1funixs.us-west-1.rds.amazonaws.com",
            'database-2.cpnta8q4u7uo.us-west-2.rds.amazonaws.com']
dbname = ["database1", "database2"]
alias_host = ["PostgreSQL AWS1", "PostgreSQL AWS2"]
port=[5432,5432]



#Using Rolling hash
def hash_string(id):
    num = 0
    for i in id:
        num = (num*PRIME + ord(i))%PRIME
    return num


def getOpenConnection(user='postgres',password='postgres',dbname='dds'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def createDB(dbname='dds'):

    #Connect to default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    #Check whether database exists or not
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' %(dbname,))
    count = cur.fetchone()[0]
    if(count == 0):
        cur.execute('CREATE DATABASE %s' %(dbname,))
    else:
        print('A database named {0} already exists in Main server'.format(dbname, ))

    cur.close()
    con.commit()
    con.close()
    createDBAWS() #for AWS server


def getOpenConnectionAWS(host , port, database="postgres"):
    return psycopg2.connect(host=host, port=port, database=database, user="postgres", password="postgres")
# 
def createDBAWS():
    #Connect to default database
    for i in range(2):
        con = getOpenConnectionAWS(host[i],port=port[i])
        con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()

    #Check whether database exists or nothost
        cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' %(dbname[i],))
        count = cur.fetchone()[0]

        if(count == 0):
            cur.execute('CREATE DATABASE %s' %(dbname[i],))
        else:
            print('A database named {0} already exists in {1} server'.format(dbname[i], alias_host[i]))

        cur.close()
        con.commit()
        con.close()

def loadRatings(rantingstablename, ratingfilepath, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS " + rantingstablename)

    cur.execute("CREATE TABLE " + rantingstablename + " (userid VARCHAR(254), rating INT)")
    

    loadout = open(ratingfilepath,'r')
    

    cur.copy_from(loadout,rantingstablename,sep=',',columns=('userid','rating'))
  
    cur.close()
    openconnection.commit()

def rangePartition(rantingstablename, numberofpartitions, openconnection, openconnectionAWS):
    name = "RangeRatingsPart"

    try:
        cursor = openconnection.cursor()
        cursorAWS = []
        for i in range(2):
            cursorAWS.append(openconnectionAWS[i].cursor())
        cursor.execute("select * from information_schema.tables where table_name='%s'" %rantingstablename)

        if(not bool(cursor.rowcount)):
            print("Please Load Ratings Table first!!!")
            return
        cursor.execute("DROP TABLE IF EXISTS RangeRatingsMetadata")
        cursor.execute("CREATE TABLE IF NOT EXISTS RangeRatingsMetadata(PartitionNum INT, HASHRating INT)")

        i = 0

        while(i<numberofpartitions):
            newTableName = name + str(i)
            cursorAWS[i].execute("DROP TABLE IF EXISTS " + newTableName)
            cursorAWS[i].execute("CREATE TABLE IF NOT EXISTS %s(UserID VARCHAR(1001), Rating INT)" %(newTableName))
            i = i + 1

        i = 0

        while(i < PRIME):
            cursor.execute("SELECT * FROM %s" %(rantingstablename))
            rows = cursor.fetchall()

            newTableName = name + str(i)
            for row in rows:
                hash_val = hash_string(row[0])
                if(hash_val == i):
                    cursorAWS[i].execute("INSERT INTO " + newTableName + " (UserID, Rating) VALUES('" + row[0] + "','" + str(row[1])+"')")
            print('Data inserted into database {0} of {1} server'.format(dbname[i], alias_host[i]))
            cursor.execute("INSERT INTO RangeRatingsMetadata (PartitionNum, HASHRating) VALUES(%d,%d)" %(i+1,i))
            i = i+1
        openconnection.commit()
        for i in range(2):
            openconnectionAWS[i].commit()

    except psycopg2.DatabaseError as e:
        if(openconnection):
            openconnection.rollback()
        for i in range(2):
            if (openconnectionAWS[i]):
                openconnectionAWS[i].rollback()
        print('Error %s' %e)
        sys.exit(1)
    except IOError as e:
        if(openconnection):
            openconnection.rollback()
        for i in range(2):
            if (openconnectionAWS[i]):
                openconnectionAWS[i].rollback()
        print('Error %s' %e)
        sys.exit(1)
    finally:
        if(cursor):
            cursor.close()
        for i in range(2):
            if (cursorAWS[i]):
                cursorAWS[i].close()

def deleteTables(rantingstablename, openconnection, openconnectionAWS):
    try:
        cursor = openconnection.cursor()
        cursorAWS = []
        for i in range(2):
            cursorAWS.append(openconnectionAWS[i].cursor())
        if(rantingstablename.upper() == 'ALL'):
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' %(table_name[0]))
            for i in range(2):
                cursorAWS[i].execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                tables = cursorAWS[i].fetchall()
                for table_name in tables:
                    cursorAWS[i].execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' %(rantingstablename))
        
        openconnection.commit()
        for i in range(2):
            openconnectionAWS[i].commit()
    
    except psycopg2.DatabaseError as  e:
        if(openconnection):
            openconnection.rollback()
        for i in range(2):
            if (openconnectionAWS[i]):
                openconnectionAWS[i].rollback()
        print('Error %s' %e)
        sys.exit(1)
    except IOError as e:
        if(openconnection):
            openconnection.rollback()
        for i in range(2):
            if (openconnectionAWS[i]):
                openconnectionAWS[i].rollback()
        print('Error %s' %e)
        sys.exit(1)
    finally:
        if(cursor):
            cursor.close()
        for i in range(2):
            if (cursorAWS[i]):
                cursorAWS[i].close()