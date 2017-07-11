# Created by : Balaji Chandrasekaran
#
#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2


# global values - used for range and roundrobin insert

tablename = ""                          #tablename of the main table
rangepartitionno = 0                    #rangepartitionno total number of range partitions
roundrobinpartitionno = 0               #roundrobinpartitionno total number of roundrobin partitions
roundrobincounter = 0                   #roundrobincounter which is used for maintaining the total number of records in roundrobin partitions


# defined functions
def loadratings(ratingstablename, filepath, openconnection):
        try:
                #print "\"DROPPING THE MAIN TABLE IF ALREADY EXISTS\""
                #cur.execute('DROP TABLE IF EXISTS {0}'.format(ratingstablename))
                global tablename
                tablename = ratingstablename
                nums = []
                with openconnection.cursor() as cur:
                        print "\"DROPPING THE MAIN TABLE IF ALREADY EXISTS\""
                        cur.execute('DROP TABLE IF EXISTS {0}'.format(ratingstablename))
                        filepath = "D:/" + filepath
                        cur.execute('CREATE TABLE {0}(userid INT, blank1 char, movieid INT, blank2 char, rating float, blank3 char, timestamp integer);'.format(ratingstablename))
                        cur.execute('COPY {0} FROM \'{1}\' (DELIMITER(\':\'));'.format(ratingstablename,filepath))
                        for i in range(1,4):
                                cur.execute('ALTER TABLE {0} DROP COLUMN {1};'.format(ratingstablename,'blank'+str(i)))
                        #cur.execute('ALTER TABLE {0} DROP COLUMN {1};'.format(ratingstablename,'blank2'))
                        #cur.execute('ALTER TABLE {0} DROP COLUMN {1};'.format(ratingstablename,'blank3'))
                        cur.execute('ALTER TABLE {0} DROP COLUMN {1};'.format(ratingstablename,'timestamp'))
                        cur.execute('SELECT * FROM {0}'.format(ratingstablename))
                        tempvar = cur.fetchall()
                        #print tempvar         		
                cur.close()
                openconnection.commit()
        except Exception as e:
                print(e)
	

def deletepartitionsandexit(openconnection):
        try:
                with openconnection.cursor() as cur:                        
                        for num in range(1,rangepartitionno+1):
                                cur.execute('DROP TABLE IF EXISTS {0}'.format("range_part"+str(num-1)))
                        for num in range(1,roundrobinpartitionno+1):
                                cur.execute('DROP TABLE IF EXISTS {0}'.format("rrobin_part"+str(num-1)))
                cur.close()
                openconnection.commit()
        except Exception as e:
                print(e)



# The below function Was used for finding range using min and max value later dropped.
def findratingsrange():
        try:
                with openconnection.cursor() as cur:
                        cur.execute('SELECT MIN({0}) FROM {1}'.format("rating",tablename))
                        minval = float(cur.fetchall()[0][0])
                        cur.execute('SELECT MAX({0}) FROM {1}'.format("rating",tablename))
                        maxval = float(cur.fetchall()[0][0])
                return float(maxval - minval)
                cur.close()
                openconnection.commit()
        except Exception as e:
                print(e)

def rangepartition(ratingstablename, numberofpartitions, openconnection):
        try:
                if numberofpartitions > 0 and isinstance(numberofpartitions,int):
                        global rangepartitionno
                        rangepartitionno = numberofpartitions
                        val = float(5)/float(numberofpartitions)
                        with openconnection.cursor() as cur:
                                for num in range(1,numberofpartitions+1):
                                        cur.execute('CREATE TABLE {0}(userid INT, movieid INT, rating float);'.format("range_part"+str(num-1)))
                                for num in range(1,numberofpartitions+1):
                                        if num == 1:
                                                cur.execute('INSERT INTO {0} SELECT * FROM {1} WHERE rating>={2} AND rating<={3};'.format("range_part"+str(num-1),tablename,0,val))
                                                curval = val

                                        else:
                                                cur.execute('INSERT INTO {0} SELECT * FROM {1} WHERE rating>{2} AND rating<={3};'.format("range_part"+str(num-1),tablename,(curval),(val*num)))
                                                curval = val*num
                        cur.close()
                        openconnection.commit()
                else:
                        print "\n\n\"PLEASE ENTER A VALID NUMBER OF PARTITION \"\n\n"
        except Exception as e:
                print(e)


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
        try:
                if numberofpartitions > 0 and isinstance(numberofpartitions,int):
                        global roundrobinpartitionno
                        roundrobinpartitionno = numberofpartitions
                        with openconnection.cursor() as cur:
                                for num in range(1,numberofpartitions+1):
                                        cur.execute('CREATE TABLE {0}(userid INT, movieid INT, rating float);'.format("rrobin_part"+str(num-1)))  
                                cur.execute('SELECT count(*) FROM {0};'.format(tablename))
                                count = cur.fetchall()[0][0]
                                global roundrobincounter
                                roundrobincounter = count
                                cur.execute('SELECT * FROM {0};'.format(tablename))
                                listrec = cur.fetchall()
                                for num in range(1,count+1):
                                        temp = num % numberofpartitions
                                        if temp == 0:
                                                temp = numberofpartitions
                                        index = num-1
                                        #print temp
                                        #print listrec[index][0]
                                        #print listrec[index][1]
                                        #print listrec[index][2]
                                        cur.execute('INSERT INTO {0} VALUES({1},{2},{3})'.format("rrobin_part"+str(temp-1),listrec[index][0],listrec[index][1],listrec[index][2]))
                        cur.close()
                        openconnection.commit()
                else:
                        print "\n\n\"PLEASE ENTER A VALID NUMBER OF PARTITION\"\n\n"
        except Exception as e:
                print(e)

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
        try:
                global rangepartitionno
                if rangepartitionno != 0:
                        val = float(5)/float(rangepartitionno)
                        partionno = 1
                        temp = 0
                        temp = val
                        #print rating
                        with openconnection.cursor() as cur:
                                for num in range(1,rangepartitionno+1):
                                        partionno = num
                                        #print temp
                                        #print partionno
                                        if rating <= temp:
                                                break
                                        temp = temp + val

                                cur.execute('INSERT INTO {0} VALUES({1},{2},{3})'.format("range_part"+str(partionno-1),userid,itemid,rating))

                                #cur.execute('SELECT * FROM {0}'.format("range_part2"))
                                #var = cur.fetchall()
                                #print var
                        cur.close()
                        openconnection.commit()
                else:
                        print   "\n\n \"THERE IS NO RANGE PARTITION, Please create proper RANGE PARTITION inorder for RANGE insert\"\n\n "
        except Exception as e:
                print(e)


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
        try:
                global roundrobincounter
                global roundrobinpartitionno                
                if roundrobinpartitionno != 0:
                        temp = roundrobincounter
                        temp = temp + 1
                        partionno = temp % roundrobinpartitionno
                        roundrobincounter = temp
                        with openconnection.cursor() as cur:
                                cur.execute('INSERT INTO {0} VALUES({1},{2},{3})'.format("rrobin_part"+str(partionno-1),userid,itemid,rating))
                        cur.close()
                        openconnection.commit()
                else:
                        print "\n\n \"THERE IS NO ROUNDROBIN PARTITION, Please create proper ROUND ROBIN PARTITION inorder for ROUND ROBIN insert \"\n\n"
        except Exception as e:
                print(e)

                
                

        

                
        
        
                        
                
                        
        
                                
                
                        
