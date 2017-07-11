#!/usr/bin/python2.7
# 
# Assignment2 Interface
#
# File_name : Assignment2_Interface.py
# Author    : Balaji Chandrasekaran

import psycopg2
import os
import sys

# Overloaded function inorder to help with the purpose of computing the query result and also for writing the query result onto the file.

def WriteHelper(cur,fi,tableName,NoOfTable,query,ratingMinValue,ratingMaxValue = None):
        try:
                if(NoOfTable > 0 and query == "range"):
                        for i in range(0, NoOfTable):
                                CurrTable = str(tableName) + str(i)
                                cur.execute("select * from %s where rating >= %s and rating <= %s"%(CurrTable,ratingMinValue,ratingMaxValue))
                                tuples = cur.fetchall()
                                for TupleRow in tuples:
                                        TupleRow = str(TupleRow).replace("(","").replace(")","")
                                        TupleRow = str(CurrTable) + ", "+str(TupleRow)
                                        fi.write(str(TupleRow) + '\n')
                elif(NoOfTable > 0 and query == "point"):
                        for i in range(0, NoOfTable):
                                CurrTable = str(tableName) + str(i)
                                cur.execute("select * from %s where rating = %s" % (CurrTable,ratingMinValue))
                                tuples = cur.fetchall()
                                for TupleRow in tuples:
                                        TupleRow = str(TupleRow).replace("(","").replace(")","")
                                        TupleRow = str(CurrTable) + ", "+str(TupleRow)
                                        fi.write(str(TupleRow) + '\n')  
                else:
                        print("Error in query type")
        except Exception as e:
                print(e)
                

# Donot close the connection inside this file i.e. do not perform openconnection.close()

def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
        try:
                cur = openconnection.cursor()                   # opened connection.
                fi  = open("RangeQueryOut.txt","w")             # opened a file inorder to output the result of the range query.
                
                #The below code is for range partition tables in case of range query
                cur.execute("select count(*) from rangeratingsmetadata")        # Finding the number of table present by range partition.
                Temp = str(cur.fetchone())
                NoOfTableRange = int(Temp.replace("(","").replace("L,)",""))    # Converting the query result into int and storing it in NoOfTableRange.
                WriteHelper(cur,fi,"rangeratingspart",NoOfTableRange,"range",ratingMinValue,ratingMaxValue)     # Calling the helper function inorder to write into the file.
                
                #The code below is for roundrobin partition tables in case of range query
                cur.execute("select partitionnum from roundrobinratingsmetadata")       # Finding the number of table present by roundrobin partition.
                Temp = str(cur.fetchone())
                NoOfTableRoundRobin = int(Temp.replace("(","").replace(",)",""))        # Converting the query result into int and storing the same.
                WriteHelper(cur,fi,"roundrobinratingspart",NoOfTableRoundRobin,"range",ratingMinValue,ratingMaxValue)   # Calling the helper function to help write into the file.
                
                #file close
                fi.close()
        except Exception as e:
                print(e)        # print exception incase of exception.
                
                
                
        #Implement RangeQuery Here.
    #pass #Remove this once you are done with implementation

def PointQuery(ratingsTableName, ratingValue, openconnection):
        try:
                cur = openconnection.cursor()                   # opened connection.
                fi  = open("PointQueryOut.txt","w")             # opened a file inorder to output the result of the range query.
                
                #The code below is for range partition tables in case of point query
                cur.execute("select count(*) from rangeratingsmetadata")
                Temp = str(cur.fetchone())
                NoOfTableRange = int(Temp.replace("(","").replace("L,)",""))
                WriteHelper(cur,fi,"rangeratingspart",NoOfTableRange,"point",ratingValue)
                
                #The code below is for roundrobin partition tables in case of point query               
                cur.execute("select partitionnum from roundrobinratingsmetadata")
                Temp = str(cur.fetchone())
                NoOfTableRoundRobin = int(Temp.replace("(","").replace(",)",""))
                WriteHelper(cur,fi,"roundrobinratingspart",NoOfTableRoundRobin,"point",ratingValue)
                
                #file close
                fi.close()
        except Exception as e:
                print(e)        # print exception incase of exception.
        
        #Implement PointQuery Here.
    #pass # Remove this once you are done with implementation
