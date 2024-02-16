import pymongo
from pymongo import ReturnDocument
import pymongo.errors as pymongo_errors
import urllib.parse

username = urllib.parse.quote_plus("copo_user")
password = urllib.parse.quote_plus("password")
myclient = pymongo.MongoClient("mongodb://%s:%s@copo_mongo:27017/" % (username, password))
mydb = myclient["copo_mongo"]

submission_collection = mydb["SubmissionCollection"]
cursor = submission_collection.find({})

for submission in cursor:
    accessions = submission.get("accessions", dict())
    if isinstance(accessions,dict):
          for key, value in accessions.items():
            if key == "sample_accessions":
                 pass
            elif isinstance(value, dict):
                  submission_collection.update_one({"_id": submission["_id"]}, 
                                                        {"$set": {f"accessions.{key}": [value]}})
                   
