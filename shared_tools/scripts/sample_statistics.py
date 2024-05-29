import dateutil.parser as parser
import json
import pymongo
from pymongo import ReturnDocument
import pymongo.errors as pymongo_errors
import urllib.parse

username = urllib.parse.quote_plus("copo_user")
password = urllib.parse.quote_plus("password")
myclient = pymongo.MongoClient("mongodb://%s:%s@copo_mongo:27017/" % (username, password))
mydb = myclient["copo_mongo"]

profile_collection = mydb["Profiles"]
sample_collection = mydb["SampleCollection"]
source_collection = mydb["SourceCollection"]


x = profile_collection.count_documents({"type":"European Reference Genome Atlas (ERGA)"})
print("Number of ERGA profiles: " + json.dumps(x))

x = profile_collection.count_documents({"type":"Darwin Tree of Life (DTOL)"})
print("Number of DTOL profiles: " + json.dumps(x))

x = profile_collection.count_documents({"type":"Aquatic Symbiosis Genomics (ASG)"})
print("Number of ASG profiles: " + json.dumps(x))

x = profile_collection.count_documents({})
print("Number of Profiles: " + json.dumps(x))

print('\n________________________________________\n')

x = source_collection.count_documents({"sample_type":"dtol_specimen"})
print("DTOL Specimens: " + json.dumps(x))

x = source_collection.count_documents({"sample_type":"asg_specimen"})
print("ASG Specimens: " + json.dumps(x))

x = source_collection.count_documents({"sample_type":"erga_specimen"})
print("ERGA Specimens: " + json.dumps(x))


print('\n________________________________________\n')


x = sample_collection.count_documents({"tol_project":"DTOL"})
print("DTOL Samples: " + json.dumps(x))

x = sample_collection.count_documents({"tol_project":"DTOL", "status":"accepted"})
print("Accepted DTOL Samples: " + json.dumps(x))

x = sample_collection.count_documents({"tol_project":"DTOL", "status":"pending"})
print("Pending DTOL Samples: " + json.dumps(x))

x = sample_collection.count_documents({"tol_project":"DTOL", "status":"rejected"})
print("Rejected DTOL Samples: " + json.dumps(x))


print('\n________________________________________\n')


x = sample_collection.count_documents({"tol_project":"ASG"})
print("ASG Samples: " + json.dumps(x))

x = sample_collection.count_documents({"tol_project":"ASG", "status":"accepted"})
print("Accepted ASG Samples: " + json.dumps(x))

x = sample_collection.count_documents({"tol_project":"ASG", "status":"pending"})
print("Pending ASG Samples: " + json.dumps(x))

x = sample_collection.count_documents({"tol_project":"ASG", "status":"rejected"})
print("Rejected ASG Samples: " + json.dumps(x))


print('\n________________________________________\n')

x = sample_collection.count_documents({"tol_project":"ERGA"})
print("ERGA Samples: " + json.dumps(x))

x = sample_collection.count_documents({"tol_project":"ERGA", "status":"accepted"})
print("Accepted ERGA Samples: " + json.dumps(x))

x = sample_collection.count_documents({"tol_project":"ERGA", "status":"pending"})
print("Pending ERGA Samples: " + json.dumps(x))

x = sample_collection.count_documents({"tol_project":"ERGA", "status":"rejected"})
print("Rejected ERGA Samples: " + json.dumps(x))

print('\n________________________________________\n')

# Get distinct items from records
# Get number of distinct SCIENTIFIC_NAME or species based on sample collection
x = sample_collection.distinct("SCIENTIFIC_NAME", {"tol_project": "ASG"})
x = len(x)
print("Number of distinct SCIENTIFIC_NAME or species for ASG samples", json.dumps(x))

#______________________________________

x = sample_collection.distinct("SCIENTIFIC_NAME", {"tol_project": "DTOL"})
x = len(x)
print("Number of distinct SCIENTIFIC_NAME or species for DTOL samples", json.dumps(x))

#______________________________________

x = sample_collection.distinct("SCIENTIFIC_NAME", {"tol_project": "ERGA"})
x = len(x)
print("Number of distinct SCIENTIFIC_NAME or species for ERGA samples", json.dumps(x))

print('\n________________________________________\n')

# Custom queries
# Get number of samples brokered between certain dates
# Replace the date strings with the desired date range
# Date period: between April 2017 and March 2023
d_from = parser.parse("2017-04-01T00:00:00+00:00")
d_to = parser.parse("2023-04-01T00:00:00+00:00")

x = sample_collection.count_documents({'tol_project': {"$in":['ASG']}, "time_created": {"$gte": d_from, "$lt": d_to}})
print("Number of ASG samples brokered between April 2017 and March 2023: " + json.dumps(x))

x = sample_collection.count_documents({'tol_project': {"$in":['DTOL']}, "time_created": {"$gte": d_from, "$lt": d_to}})
print("Number of DTOL samples brokered between April 2017 and March 2023: " + json.dumps(x))

x = sample_collection.count_documents({'tol_project': {"$in":['ERGA']}, "time_created": {"$gte": d_from, "$lt": d_to}})
print("Number of ERGA samples brokered between April 2017 and March 2023: " + json.dumps(x))

x = sample_collection.count_documents({'tol_project': {"$in":['ASG','DTOL','ERGA']}, "time_created": {"$gte": d_from, "$lt": d_to}})
print("Total number of ASG, DTOL and ERGA samples brokered between April 2017 and March 2023: " + json.dumps(x))