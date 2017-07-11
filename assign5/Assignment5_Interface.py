#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: Balaji Chandrasekaran
#

from pymongo import MongoClient
import os
import sys
import json
import re
import codecs
import math


def distance_in_miles(latitude1, longitude1, latitude2, longitude2):
    R = 3959 #Miles As given.
    latR1 = math.radians(latitude1)
    latR2 = math.radians(latitude2)
    disLat = math.radians(latitude2-latitude1);
    disLong = math.radians(longitude2-longitude1);

    a = math.sin(disLat/2) * math.sin(disLat/2) + math.cos(latR1) * math.cos(latR2) * math.sin(disLong/2) * math.sin(disLong/2);
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a));
    d = R * c;
    return d

# The below function is used for finding all buisness in the given city - "cityToSearch"
# from "collection" and save it in a location specified by the "saveLocation1"
# Result - Stores Full address, city, state of business in the following format
# Name$FullAddress$City$State.

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    cursor = collection.find({"city":re.compile(cityToSearch, re.IGNORECASE)})
    try:
        fileWrite = codecs.open(saveLocation1,'a',encoding='utf-8')
        # truncating the file to make sure it is empty.
        fileWrite.truncate()
        for result in cursor:
	    # making all the fields in the result to upper and writing the line into the file.
	    fileWrite.write("%s$%s$%s$%s\n" % (result["name"].upper(), result["full_address"].upper().replace("\n",","), result["city"].upper(), result["state"].upper()))
            #print line
        cursor.close()
    except Exception as e:
        print e

    

# This function searches the collection given to find name of all the business
# present in the maxDistance from the given myLocation and save them to saveLocation2
# Result - Stores only the name of each buisness to the saveLocation2 	
	
def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    lat = myLocation[0]
    longi = myLocation[1]
    cursor = collection.find({"categories": {"$in":categoriesToSearch}})
    try:
        fileWrite = codecs.open(saveLocation2,'a',encoding='utf-8')
        # truncating the file to make sure it is empty.
        fileWrite.truncate()
        for result in cursor:
            distance = distance_in_miles(float(lat), float(longi), float(result["latitude"]), float(result["longitude"]))
            if distance <= maxDistance:
                 # making all the fields in the result to upper and writing the line into the file.
                fileWrite.write("%s\n" % result["name"].upper())
        cursor.close()
    except Exception as e:
        print e
    

