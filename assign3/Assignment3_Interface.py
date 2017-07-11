#!/usr/bin/python2.7
#
# Assignment3 Interface
# AUTHOR - BALAJI CHANDRASEKARAN

import psycopg2
import os
import sys
import threading
import datetime
import time


##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################

#UTILITY FUNCTION:- inorder to delete the temp table that are created for various threads inorder to partition and work on and also for the purpose of joining the threads.
def threadjoinFunc(threads, outputtable,cur):
	try:
		ct = 1;
		for x in threads:
                        if ct == 1:
                                cur.execute("create table %s as select * from temptable1" % (outputtable))
                        else:
                                cur.execute("insert into %s select * from temptable%s" % (outputtable, ct))
                        ct+=1
	except Exception as ex:
		print(ex)
		
def delTempTableFunc(cur):
	try:
		for i in range(1,6):
			cur.execute("drop table temptable%s" % i)
	except Exception as ex:
		print(ex)

#PARALLEL SORT:-

def SortFunc(table_name, InputTable, SortingColumnName, minval, maxval, openconnection):
	try:
		cur = openconnection.cursor()
		cur.execute("create table %s as select * from %s where %s >= %s and %s < %s order by %s" % (table_name, InputTable, SortingColumnName, minval, SortingColumnName, maxval, SortingColumnName))
		cur.close()
		openconnection.commit()
	except Exception as ex:
		print(ex)
		
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
	try:
		cur = openconnection.cursor()
		cur.execute("select min(%s), max(%s) from %s" % (SortingColumnName, SortingColumnName, InputTable))
		temp = cur.fetchone()
		minval = temp[0]
		maxval = temp[1]
		split = (maxval - minval)/5
		threads = []
		for i in range(1,6):
			if i == 5:
				table_name = "temptable%s" % i
				maxval = temp[1] + 1
			else:
				table_name = "temptable%s" % i
				maxval = minval + split
			t = threading.Thread(target = SortFunc, args = (table_name, InputTable, SortingColumnName, minval, maxval, openconnection))
			threads.append(t)
			t.start()
			minval = maxval
		openconnection.commit()
		threadjoinFunc(threads,OutputTable,cur)
		delTempTableFunc(cur)		
		cur.close()
		openconnection.commit()
	except Exception as e:
			print(e)
		

#PARALLEL JOIN
	
	
def JoinFunc(table_name, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, minvalue, maxvalue, openconnection):
	try:
		cur = openconnection.cursor()
		qr = "select "
		if Table1JoinColumn == Table2JoinColumn:
			cur.execute("select column_name from information_schema.columns where table_name = '%s'" % (InputTable1))
			tempcolumn = cur.fetchall()
			for column in tempcolumn:
				clname = str(column).replace("',)","").replace("('","")
				if clname != Table1JoinColumn:
					qr += "t1.%s," % clname
			joinquery = "create table %s as %s t2.* from %s t1 inner join %s t2 on t1.%s = t2.%s and t1.%s >= %s and t1.%s < %s and t2.%s >= %s and t2.%s < %s" % (table_name, qr, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, Table1JoinColumn, minvalue, Table1JoinColumn, maxvalue, Table2JoinColumn, minvalue, Table2JoinColumn, maxvalue)
		else:
			joinquery = "create table %s as select * from %s t1 inner join %s t2 on t1.%s = t2.%s and t1.%s >= %s and t1.%s < %s and t2.%s >= %s and t2.%s < %s" % (table_name, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, Table1JoinColumn, minvalue, Table1JoinColumn, maxvalue, Table2JoinColumn, minvalue, Table2JoinColumn, maxvalue)
		cur.execute(joinquery)
		cur.close()
		openconnection.commit()
	except Exception as ex:
		print(ex)
			
			
def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    try:
        cur = openconnection.cursor()        
        cur.execute("select min(%s), max(%s) from %s" % (Table1JoinColumn, Table1JoinColumn, InputTable1))
        temp = cur.fetchone()
        minvalue = temp[0]
        maxvalue = temp[1]
        split = (maxvalue-minvalue)/5
        threads = []
        for i in range(1,6):    
            if i == 5:
                    table_name = "temptable%s" % i
                    maxvalue = temp[1] + 1
            else:
                    table_name = "temptable%s" % i
                    maxvalue = minvalue + split
            t = threading.Thread(target = JoinFunc, args = (table_name, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, minvalue, maxvalue, openconnection))
            threads.append(t)
            t.start()
            time.sleep(0.2)
            minvalue = maxvalue
        openconnection.commit()
	threadjoinFunc(threads,OutputTable,cur)	
	delTempTableFunc(cur)
        cur.close()
        openconnection.commit()
    except Exception as e:
        print (e)



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    ct = cur.fetchone()[0]
    if ct == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment2
	print "Creating Database named as ddsassignment2"
	createDB();
	
	# Getting connection to the database
	print "Getting connection from the ddsassignment2 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);
	
	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
