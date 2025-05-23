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
    old_accessions = submission.get("accessions", dict())
    sample_accessions = dict()
    if isinstance(old_accessions,dict):
          sample_accessions = old_accessions.get("sample_accessions", dict())
    
    accessions = []
    print("updating submission: ", submission["_id"])
    for key, sample_accession in sample_accessions.items():
            accessions.append(dict(sample_accession=sample_accession.get("sraAccession",""), 
                                   biosample_accession=sample_accession.get("biosampleAccession",""), 
                                    sample_id=key))
            sample = submission_collection.update_one({"_id": submission["_id"]}, 
                                                        {"$addToSet": {"accessions.sample": {"$each": accessions}}})
