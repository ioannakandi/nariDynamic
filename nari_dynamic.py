# -*- coding: utf-8 -*-
"""
Created on Thu Jan 13 14:27:36 2022

@author: Ioanna
"""

import pymongo
from pymongo import MongoClient, GEO2D
from pymongo.errors import DuplicateKeyError
from flask import Flask, render_template, flash, request, Markup, session, Response, send_file
import time, os, sys
from flask_cors import CORS, cross_origin
import json
import csv
import random

# Connect to our local MongoDB
mongodb_hostname = os.environ.get("MONGO_HOSTNAME","localhost")
client = MongoClient('mongodb://'+mongodb_hostname+':27017/')

# Choose nari database
db = client['nari']
marine_data = db['marine data']


# App config.
app = Flask(__name__, static_url_path='',
            static_folder='templates',
            template_folder='templates')
DEBUG = True
app.config.from_object(__name__)
app.config['SECRET_KEY'] = '7d441f27d441f27567d441f2b6176a'
CORS(app)

#homepage 
@app.route("/")
@cross_origin()
def nari_home():
    return render_template("nari_home.html")

# endpoint for query1: return vessel details through sourcemmsi search
@app.route("/vesselSearch")
@cross_origin()
def vesselSearch():
    if request.method == 'GET':
        sourcemmsi = request.args.get('sourcemmsi')
        query = {'sourcemmsi':int(sourcemmsi)} #converts sourcemmsi input from str to int, so it can be identified by mongo
        result_cursor = marine_data.find(query, {"_id": 0}).limit(4) #because each vessel/sourcemmsi corresponds to multiple documents
                                                                     # the table filling has been limited to 4 sourcemmsi corresponding documents
        # convert cursor object to python list
        list_cur = list(result_cursor)
        return Response(json.dumps(list_cur), status=200, mimetype="application/json") 
  
 # endpoint for query2: return up to 5 vessels with speed greater than the one given by the user at a specific time, also provided by the user
@app.route("/knotSearch")
@cross_origin()
def knotSearch():
    if request.method == 'GET':
        speedoverground = request.args.get('speedoverground')
        t = request.args.get('t')
        query = { "speedoverground": { "$gt": int(speedoverground) }, "t":t }
        result_cursor = marine_data.find(query, {"_id": 0}).limit(5)
        # convert cursor object to python list
        list_cur = list(result_cursor)
        return Response(json.dumps(list_cur), status=200, mimetype="application/json") 
    
#endpoint for query3: return a vessel's route through sourcemmsi    
#in order for this query to be executed successfully, the marine_data collection has to be skimmed through via sourcemmsi 
#and sorted by time in ascending order.
#time (ascending) is necessary because a vessel's position varies from timestamp to timestamp. 
#Again, because one sourcemmsi has multiple documents, they have been limited to 10. 
@app.route("/getCourse")
@cross_origin()
def getCourse():
    if request.method == 'GET':
        sourcemmsi = request.args.get('sourcemmsi')
        query = { "sourcemmsi": int(sourcemmsi)}
        result_cursor = marine_data.find(query, {"_id": 0}).sort("t", 1).limit(10)
        # convert cursor object to python list
        list_cur = list(result_cursor)
        return Response(json.dumps(list_cur), status=200, mimetype="application/json") 
    
    
#endpoint for query4: locating vessels 8km away from given coordinates
@app.route("/vesselLocation",methods=['GET'])
@cross_origin()
def vesselLocation():
    if request.method == 'GET':
        #get the parameters from the request/user
        lat = request.args.get('lon')
        lon = request.args.get('lat')
        
        #find the sourcemmsi of the vessels whith distance 8km from given coordinates
        #"merging" lon and lat variables into one (coordinates) and setting maxDistance away from given coordinates as 8km 
        #via geoNear aggregation pipeline. 
        #returning chosen/necessary fields: sourcemmsi, speedoverground, courseoverground, trueheading for each vessel corresponding to the user's input
        pipeline = [{"$geoNear": {"near": {"type":"Point", "coordinates": [int(lat), int(lon)]},"distanceField":"dist.calculated", "maxDistance":8000, "includeLocs":"dist.location", "sphere":"true", "key": "position" }}
                    ,{ "$project": { "sourcemmsi": 1, "speedoverground":1,"courseoverground":1,
                                    "trueheading":1, "_id":0} }]
        result_list = list(marine_data.aggregate(pipeline))
        
        return Response(json.dumps(result_list), status=200, mimetype="application/json") 


#enpoint for creating indexes to minimize query execution time
@app.route("/generateIndex",methods=['GET'])
@cross_origin()
def generateIndex():
    if request.method == 'GET':
        
        #1) 2dsphere index for coordinates in query4. It's in comment form because it doesn't run. Instead it was created through Compass
        #db.["marine data"].updateMany({},[{"$set":{ "position" : {type:"Point", coordinates:["$lon", "$lat"] } }} ] )
        #create the coordinates index that contains both longtitude and latitude (it wil be used for
        #searching the collection based on coordinates)
        #marine_data.create_index([("position", GEO2D)])
        
        #2) sourcemmsi index 
    
        marine_data.create_index([('sourcemmsi',pymongo.ASCENDING)],
                             name='sourcemmsi')
        
        #3) speedoverground index 
        marine_data.create_index([('speedoverground',pymongo.ASCENDING)],
                             name='speedoverground')
    return Response("Message:Indexes genetared.", status=200, mimetype="application/json")
        


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)