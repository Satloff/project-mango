#!/usr/bin/python

import urllib2
import simplejson
import cStringIO

import uuid
import os
import threading
import time

import boto
import boto.s3.connection
from boto.s3.key import Key

import pymysql.cursors
import sys
import json
import ast
import csv

printBucket = False
debug = False
Run = 1

onThrain = False

if onThrain:
	thrainPath = "[PATH]"
else:
	thrainPath = ""


#-------- Load Tokens for Instagram and Bucket--------#
config = {}
execfile(thrainPath+"config.py", config)

###############################
##   ----- AWS Bucket -----  ##
##         accessKey  	      ##
##         secretKey 	      ##
##       testBucketName 	 ##
##					      ##
##   ----- Instagram ------  ##
##        accessToken        ##
## 					      ##
##     ------ EC2 ------     ##
##            host           ##
##            user           ##
##             db            ##
##          password         ##
###############################

#-----------------------------------------------------------#
#--------------------Connect To Bucket----------------------#
#-----------------------------------------------------------#
def bucket(link, dest, config):

	#-------- Begin Connection --------#
	conn = boto.connect_s3(
	        aws_access_key_id = config["accessKey"],
	        aws_secret_access_key = config["secretKey"])

	if printBucket:
		print "I have successfully connected to the S3 space where the buckets live"
		print


	#-------- List Active Bucscrape(
		print "Now here is the list of the buckets:"
		print

		for bucket in conn.get_all_buckets():
			print "{name}\t{created}".format(name = bucket.name, created = bucket.creation_date)


	#-------- List Contents of Buckets --------#

	if printBucket:
		print
		print "This is the list of the stuff in the buckets:"
		print
		for key in bucket.list():
			print bucket.name,":",key.name,key.size,key.last_modified


	#-------- Save to Bucket --------#
	b = conn.get_bucket(config["testBucketName"]) # Connect to our test bucket
	k = Key(b) #  Prepare to create a new file - 'key' means 'filename'
	k.key = dest # new Filename
	k.set_contents_from_filename(link) # source file


	#-------- Download Bucket Object --------#
	#k = bucket.get_key({NAME OF FILE})
	#k.get_contents_to_filename({SAVE FILE PATH})
	return


#-----------------------------------------------------------#
#----------------Connect To EC2 for MySQL-------------------#
#-----------------------------------------------------------#

def sql_connect(data, hashtags):

	# Connect to the database
	connection = pymysql.connect(host=config['host'],
							user=config['user'],
							password=config['password'],
							db=config['db'],
							charset='utf8mb4',
							cursorclass=pymysql.cursors.DictCursor)

	try:
		with connection.cursor() as cursor:
			# Create a new record
			sql = "INSERT INTO `instagram_data` (`id`, `image_link`, `image_name`, `latitude`, `longitude`, `location_name`, `bucket_url`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
			cursor.execute(sql, (data[0][2], data[0][0], data[0][1], data[1], data[2], data[3], data[5]))

			if len(hashtags) > 0:
				for i in range(len(hashtags)):
					sql = "INSERT INTO `instagram_data_tags` (`image_name`, `tag`) VALUES (%s, %s)"
					cursor.execute(sql, (data[0][2], hashtags[i]))

		connection.commit()
	finally:
		connection.close()
	return


#-----------------------------------------------------------#
#-----------------------Return JSON-------------------------#
#-----------------------------------------------------------#
def search(max, min, radius, lat, lng):
	fetcher = urllib2.build_opener()
	try:
		accessToken = config["accessToken"]


		searchUrl = "https://api.instagram.com/v1/media/search?max_timestamp=%s&min_timestamp=%s&distance=%s&lat=%s&lng=-%s&client_id=%s" % (max, min, radius, lat, lng, accessToken)
		if debug:
			print "trying gramfeed's TOKEN"
		f = fetcher.open(searchUrl)

	except urllib2.HTTPError, err:
		if err.code == 429:
			try:
				accessToken = config["accessToken2"]
				searchUrl = "https://api.instagram.com/v1/media/search?max_timestamp=%s&min_timestamp=%s&distance=%s&lat=%s&lng=-%s&client_id=%s" % (max, min, radius, lat, lng, accessToken)
				if debug:
					print "trying worldcam's TOKEN"
				f = fetcher.open(searchUrl)

			except urllib2.HTTPError, err:
				if err.code == 429:
					try:
						accessToken = config["accessToken3"]
						searchUrl = "https://api.instagram.com/v1/media/search?max_timestamp=%s&min_timestamp=%s&distance=%s&lat=%s&lng=-%s&client_id=%s" % (max, min, radius, lat, lng, accessToken)
						if debug:
							print "trying instabams's TOKEN"
						f = fetcher.open(searchUrl)
					except urllib2.HTTPError, err:
						if err.code == 429:
							try:
								accessToken = config["accessToken4"]
								searchUrl = "https://api.instagram.com/v1/media/search?max_timestamp=%s&min_timestamp=%s&distance=%s&lat=%s&lng=-%s&client_id=%s" % (max, min, radius, lat, lng, accessToken)
								if debug:
									print "trying Theo's TOKEN"
								f = fetcher.open(searchUrl)
							except urllib2.HTTPError, err:
								if err.code == 429:
									if debug:
										print "All tokens have expired"

	deserialized_output = simplejson.load(f)

	if debug:
		print deserialized_output

	return deserialized_output

#-----------------------------------------------------------#
#------------------- Read CSV to a list---------------------#
#-----------------------------------------------------------#
def readIn(file):
	f = open(file)
	names = []
	for row in csv.reader(f):
		for each in row:
			names.append(each)
	f.close()
	return names

#-----------------------------------------------------------#
#---------------Check if list contains value----------------#
#-----------------------------------------------------------#
def checkNames(name, list):
	for each in list:
		if name == each:
			return True
		#else:
		#	continue
	return

#-----------------------------------------------------------#
#------------------=-Write list to CSV----------------------#
#-----------------------------------------------------------#
def writeOut(file, list):

	f = open(file, 'wb')
	temp = []
	#print list
	for each in list:
		#print each
		temp.append(str(each))

	f.write(','.join(temp))
	f.close()

	#print temp

	return


# inherit the Thread class
class scrape(threading.Thread):
    def __init__(self, threadNum, maximum, minimum, radius, lat, lng, fileName): # set up the task
        threading.Thread.__init__(self)
        self.threadNum = threadNum
        self.max = maximum
        self.min = minimum
        self.radius = radius
        self.lat = lat
        self.lng = lng
        self.file = fileName

    def run(self): # called by the parent start method
		#global latitude

        #-------- Declare Values --------#
    	data = [["Image Link","New Image Name","ID"], "Latitude","Longitude","Location Name", ["hashtags"], "bucket_url"]

    	deserialized_output = search(self.max, self.min, self.radius, self.lat, self.lng)

    	length = len(deserialized_output['data'])
    	#print length

    	#-------- Save Image and Data --------#
    	for i in range(length):
    		hashtags = []
    		value = "nothing"

    		#-------- Isolate Features of JSON --------#
    		image_url = deserialized_output['data'][i]['images']['standard_resolution']['url']

    		hashtags_len = len(deserialized_output["data"][i]["tags"])
    		for j in range(hashtags_len):
    			tag = deserialized_output["data"][i]["tags"][j]
    			hashtags.append(tag)

			latitude = deserialized_output['data'][i]['location']['latitude']
			#print latitude
    		name = deserialized_output['data'][i]['location']['name']
    		longitude = deserialized_output['data'][i]['location']['longitude']

    		#-------- Store Data in Array --------#
    		data[0][0] = image_url
    		data[1] = latitude
    		data[2] = longitude
    		data[3] = name
    		data[4] = hashtags

    		#-------- Store Images in Bucket --------#
    		filename_type = "jpg"
    		filename = data[0][0].split('/')[-1].split('.')[0]
    		file_uuid = str(uuid.uuid4())
    		unique_filename = "instagram_images/" + file_uuid + "." + filename_type #this is the UUID for the image name
    		bucket_url = "https://s3-us-west-2.amazonaws.com/theoandwalker/instagram_images/%s.%s" % (file_uuid, filename_type)

    		image = urllib2.urlopen(data[0][0]).read() # store the image locally temporarily

    		#-------- Add New Data to Array --------#
    		data[0][1] = unique_filename
    		data[0][2] = file_uuid
    		data[5] = bucket_url
    		if debug: print data[5]

    		#-------- Check for duplicates before entry --------#
    		newList = readIn(self.file)
    		#print newList

    		boolean = checkNames(filename, newList)
    		if boolean:
    			if debug: print "there was a match"
    			continue
    		else:
				try:
					newList.append(filename)
					item = newList.pop(0)
	    			#print item

				except:
					newList.append(filename)


    		#print newList
    		writeOut(self.file, newList)


    		f = open(unique_filename,'wb+') #add temporary local file for writing to bucket
    		f.write(image)
    		f.close()

    		if debug: print unique_filename

    		bucket(unique_filename, unique_filename, config)
    		# right now a temporary file is stored
    		# I don't like it... i tried to pass it the image binary, but that doesn't work

    		try:
    			os.remove(unique_filename) # delete temporary file from local machine
    		except:
    			print "failed"

    		#-------- Add Data to MySQL --------#
    		sql_connect(data, hashtags)

    		#print data
    		#print "%s - %s" % (threadNum, unique_filename)

		#print "%s - %s" % (self.threadNum, unique_filename)


    	if debug:
    		print "%s - %s" % (self.threadNum, unique_filename)
    		print "%s - %s" % (self.threadNum, int(time.time())) # this prints the unix timestamp

    	#print "finished looping"
    	return


scrape("thread-0", '', '', 1000, 40.7127, 74.0059, thrainPath+"files/1.csv").start()
# scrape("thread-1", '', '', 1000, 34.05, 118.25, thrainPath+"files/2.csv").start()
# scrape("thread-2", '', '', 1000, 41.8369, 87.6847, thrainPath+"files/3.csv").start()
# scrape("thread-3", '', '', 1000, 29.7604, 95.3698, thrainPath+"files/4.csv").start()
# scrape("thread-4", '', '', 1000, 39.95, 75.1667, thrainPath+"files/5.csv").start()
# scrape("thread-5", '', '', 1000, 33.45, 112.0667, thrainPath+"files/6.csv").start()
# scrape("thread-6", '', '', 1000, 29.4167, 98.5, thrainPath+"files/7.csv").start()
# scrape("thread-7", '', '', 1000, 32.715, 117.1625, thrainPath+"files/8.csv").start()
# scrape("thread-8", '', '', 1000, 32.7767, 96.797, thrainPath+"files/9.csv").start()
# scrape("thread-9", '', '', 1000, 37.3382, 121.8863, thrainPath+"files/10.csv").start()
# scrape("thread-10", '', '', 1000, 30.25, 30.25, thrainPath+"files/11.csv").start()
# scrape("thread-11", '', '', 1000, 30.3369, 81.6614, thrainPath+"files/12.csv").start()
# scrape("thread-12", '', '', 1000, 37.7833, 122.4167, thrainPath+"files/13.csv").start()
# scrape("thread-13", '', '', 1000, 39.791, 86.148, thrainPath+"files/14.csv").start()
# scrape("thread-14", '', '', 1000, 39.9833, 82.9833, thrainPath+"files/15.csv").start()
# scrape("thread-15", '', '', 1000, 32.7574, 97.3332, thrainPath+"files/16.csv").start()
# scrape("thread-16", '', '', 1000, 35.2269, 80.8433, thrainPath+"files/17.csv").start()
# scrape("thread-17", '', '', 1000, 42.3314, 83.0458, thrainPath+"files/18.csv").start()
# scrape("thread-18", '', '', 1000, 31.7903, 106.4233, thrainPath+"files/19.csv").start()
# scrape("thread-19", '', '', 1000, 47.6097, 122.3331, thrainPath+"files/20.csv").start()
# scrape("thread-20", '', '', 1000, 39.7392, 104.9903, thrainPath+"files/21.csv").start()
# scrape("thread-21", '', '', 1000, 38.9047, 77.0164, thrainPath+"files/22.csv").start()
# scrape("thread-22", '', '', 1000, 35.1174, 89.9711, thrainPath+"files/23.csv").start()
# scrape("thread-23", '', '', 1000, 42.3601, 71.0589, thrainPath+"files/24.csv").start()
# scrape("thread-24", '', '', 1000, 36.1667, 86.7833, thrainPath+"files/25.csv").start()
# scrape("thread-25", '', '', 1000, 39.2833, 76.6167, thrainPath+"files/26.csv").start()
# scrape("thread-26", '', '', 1000, 35.4822, 97.535, thrainPath+"files/27.csv").start()
# scrape("thread-27", '', '', 1000, 45.52, 122.6819, thrainPath+"files/28.csv").start()
# scrape("thread-28", '', '', 1000, 36.1215, 115.1739, thrainPath+"files/29.csv").start()
# scrape("thread-29", '', '', 1000, 38.25, 85.7667, thrainPath+"files/30.csv").start()
# scrape("thread-30", '', '', 1000, 43.05, 87.95, thrainPath+"files/31.csv").start()
# scrape("thread-31", '', '', 1000, 35.1107, 106.61, thrainPath+"files/32.csv").start()
# scrape("thread-32", '', '', 1000, 32.2217, 110.9264, thrainPath+"files/33.csv").start()
# scrape("thread-33", '', '', 1000, 36.75, 119.7667, thrainPath+"files/34.csv").start()
# scrape("thread-34", '', '', 1000, 38.5556, 121.4689, thrainPath+"files/35.csv").start()
# scrape("thread-35", '', '', 1000, 33.7683, 118.1956, thrainPath+"files/36.csv").start()
# scrape("thread-36", '', '', 1000, 39.0997, 94.5783, thrainPath+"files/37.csv").start()
# scrape("thread-37", '', '', 1000, 33.415, 111.8314, thrainPath+"files/38.csv").start()
# scrape("thread-38", '', '', 1000, 33.755, 84.39, thrainPath+"files/39.csv").start()
# scrape("thread-39", '', '', 1000, 36.8506, 75.9779, thrainPath+"files/40.csv").start()
# scrape("thread-40", '', '', 1000, 41.25, 96, thrainPath+"files/41.csv").start()
# scrape("thread-41", '', '', 1000, 38.8673, 104.7607, thrainPath+"files/42.csv").start()
# scrape("thread-42", '', '', 1000, 35.7806, 78.6389, thrainPath+"files/43.csv").start()
# scrape("thread-43", '', '', 1000, 25.7753, 80.2089, thrainPath+"files/44.csv").start()
# scrape("thread-44", '', '', 1000, 37.8044, 122.2708, thrainPath+"files/45.csv").start()
# scrape("thread-45", '', '', 1000, 44.9778, 93.265, thrainPath+"files/46.csv").start()
# scrape("thread-46", '', '', 1000, 36.1314, 95.9372, thrainPath+"files/47.csv").start()
# scrape("thread-47", '', '', 1000, 41.4822, 81.6697, thrainPath+"files/48.csv").start()
# scrape("thread-48", '', '', 1000, 37.6889, 97.3361, thrainPath+"files/49.csv").start()
# scrape("thread-49", '', '', 1000, 29.95, 90.0667, thrainPath+"files/50.csv").start()
# scrape("thread-50", '', '', 1000, 32.705, 97.1228, thrainPath+"files/51.csv").start()
