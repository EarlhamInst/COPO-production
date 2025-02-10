import dateutil.parser as parser
import pymongo
import urllib.parse

username = urllib.parse.quote_plus('copo_user')
password = urllib.parse.quote_plus('password')
myclient = pymongo.MongoClient('mongodb://%s:%s@copo_mongo:27017/' % (username, password))
mydb = myclient['copo_mongo']

profile_collection = mydb['Profiles']
sample_collection = mydb['SampleCollection']
source_collection = mydb['SourceCollection']

tol_profile_types = ['asg', 'dtol', 'erga']
tol_profile_types_upper = [x.upper() for x in tol_profile_types]

for x in tol_profile_types:
    count = profile_collection.count_documents({'type': x})
    print(f'Number of {x} profiles: {count}')

x = profile_collection.count_documents({})
print(f'Total number of profiles: {x}')

print('\n________________________________________\n')

for x in tol_profile_types:
    count = source_collection.count_documents({'sample_type': x + '_specimen'})
    print(f'{x.upper()} specimens: {count}')

print('\n________________________________________\n')

for x in tol_profile_types_upper:
    count = sample_collection.count_documents({'tol_project': x})
    print(f'{x} samples: {count}')
    
    count = sample_collection.count_documents({'tol_project': x, 'status': 'accepted'})
    print(f'Accepted {x} samples: {count}')
    
    count = sample_collection.count_documents({'tol_project': x, 'status': 'pending'})
    print(f'Pending {x} samples: {count}')
    
    count = sample_collection.count_documents({'tol_project': x, 'status': 'rejected'})
    print(f'Rejected {x} samples: {count}')
    
    print('\n________________________________________\n')
    
#______________________________________

# Get distinct items from records
# Get number of distinct 'SCIENTIFIC_NAME' or species based on sample collection
for x in tol_profile_types_upper:
    output = sample_collection.distinct('SCIENTIFIC_NAME', {'tol_project': x})
    print(f'Number of distinct "SCIENTIFIC_NAME" or species for {x} samples: {len(output)}')
    
print('\n________________________________________\n')

#______________________________________

# Custom queries
# Get number of samples brokered between certain dates
# Replace the date strings with the desired date range
# Date period: between April 2017 and March 2023
d_from = parser.parse('2017-04-01T00:00:00+00:00')
d_from_mm_yyyy = d_from.strftime('%B %Y')

d_to = parser.parse('2023-04-01T00:00:00+00:00')
d_to_mm_yyyy = d_to.strftime('%B %Y')

for x in tol_profile_types_upper:
    count = sample_collection.count_documents({'tol_project': x, 'time_created': {'$gte': d_from, '$lt': d_to}})
    print(f'Number of {x} samples brokered between {d_from_mm_yyyy} and {d_to_mm_yyyy}: {count}')

count = sample_collection.count_documents({'tol_project': {'$in': tol_profile_types_upper}, 'time_created': {'$gte': d_from, '$lt': d_to}})
profile_types_str = ', '.join(tol_profile_types_upper)
print(f'\nTotal number of {profile_types_str} samples brokered between {d_from_mm_yyyy} and {d_to_mm_yyyy}: {count}')