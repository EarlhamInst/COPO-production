#python generate_bia_filelist.py imagelist_downloaded_from_bia.tsv
#output will be ERGA_yyyymmdd.tsv
import pymongo
import urllib.parse
import pandas as pd
import re
import os
import sys
import datetime

def get_env(env_key):
    env_value = str()
    if env_key in os.environ:
        env_value = os.getenv(env_key)

    # resolve for file assignment
    file_env = os.environ.get(env_key + '_FILE', str())
    if len(file_env) > 0:
        try:
            with open(file_env, 'r') as mysecret:
                data = mysecret.read().replace('\n', str())
                env_value = data
        except:
            pass
    return env_value


if len(sys.argv) < 2:
    print("Usage: python generate_bia_filelist.py <path_to_imagefilenamelist>")
    sys.exit(1)

input_file = sys.argv[1]
now = datetime.datetime.now()
output_file = "ERGA_"+ now.strftime("%Y%m%d") + ".tsv"

# Configure MongoDB database then, connect to it
username = urllib.parse.quote_plus(get_env('MONGO_USER'))
password = urllib.parse.quote_plus(get_env('MONGO_USER_PASSWORD'))
host = urllib.parse.quote_plus(get_env('MONGO_HOST'))
port = urllib.parse.quote_plus(get_env('MONGO_PORT'))
mongoClient = pymongo.MongoClient(
    f'mongodb://{username}:{password}@{host}:{port}/')
mongoDB = mongoClient['copo_mongo']

df = pd.read_csv(input_file, sep="\t")
f = lambda x : re.search('(?<=/)[A-Z0-9]+', x).group(0)

df["biosampleAccession"] = df["Files"].apply(f)
biosampleAccessions = df["biosampleAccession"].unique()

source_collection = mongoDB["SourceCollection"]
cursor = source_collection.find({"biosampleAccession": {"$in": list(biosampleAccessions)}})
files = []
for qm in cursor:
    file = {
        "biosampleAccession": qm["biosampleAccession"],
        "BioSamples link": f"https://www.ebi.ac.uk/biosamples/samples/{qm['biosampleAccession']}", 
        "BioSamples accession ID": qm["biosampleAccession"],
        "NCBI taxononmy ID": qm["TAXON_ID"],
        "geographic location (latitude)": qm["DECIMAL_LATITUDE"],
        "geographic location (longitude)": qm["DECIMAL_LONGITUDE"],
        "geographic location (region and locality)": qm["COLLECTION_LOCATION"],
        "habitat": qm["HABITAT"],
        "sample collection device or method": qm["DESCRIPTION_OF_COLLECTION_METHOD"]
    }
    files.append(file)

df2 = pd.DataFrame.from_records(files)

df3 = pd.merge(df, df2, left_on='biosampleAccession', right_on='biosampleAccession')
df3.drop(columns=["biosampleAccession"], inplace=True)

df3.to_csv(output_file, sep="\t", index=False)
