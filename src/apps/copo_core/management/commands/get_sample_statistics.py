import os
import pandas as pd
import pymongo
import urllib.parse

from datetime import datetime, timezone
from django.contrib.auth.models import User
from django.core.management import BaseCommand
from django.http import HttpResponse
from tabulate import tabulate

from src.apps.api.utils import validate_date_from_api

# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    # Show this when the user types help
    help = 'Get statistics of records in COPO'

    def handle(self, *args, **options):
        self.stdout.write('Running statistics...')

        self.setup_mongodb_connection()
        self.get_profile_statistics()
        self.get_specimen_statistics()
        self.get_sample_statistics()
        self.get_distinct_items()
        self.get_sample_statistics_between_dates()
        self.rank_genomic_profiles_and_get_owner_email()

    # _______________________

    # MongoDB Connection
    def setup_mongodb_connection(self):
        username = urllib.parse.quote_plus('copo_user')
        password = urllib.parse.quote_plus('password')
        myclient = pymongo.MongoClient('mongodb://%s:%s@copo_mongo:27017/' % (username, password))
        mydb = myclient['copo_mongo']

        self.non_tol_sample_types = {'isasample': 'genomics'}
        self.tol_sample_types = ['asg', 'dtol', 'erga']
        self.tol_specimen_types = [ x + '_specimen' for x in self.tol_sample_types]

        self.sample_status = ['accepted', 'pending', 'rejected']
        self.sample_types = list(self.non_tol_sample_types.keys()) + self.tol_sample_types

        self.profile_collection = mydb['Profiles']
        self.sample_collection = mydb['SampleCollection']
        self.source_collection = mydb['SourceCollection']
    # ______________________________________

    # Count the number of profile records
    def get_profile_statistics(self):
        profile_types = list(self.non_tol_sample_types.values()) + self.tol_sample_types
        print('\n')
        for x in profile_types:
            count = self.profile_collection.count_documents({'type': x})
            print(f'{x.upper()} profile count: {count}')

        count = self.profile_collection.count_documents({})
        print(f'Total number of profiles: {count}')

        print('\n________________________________________\n')

    # ______________________________________

    # Count the number of sources/specimens records
    def get_specimen_statistics(self):
        count = self.source_collection.count_documents({'sample_type': {'$nin': self.tol_specimen_types}})
        print(f'GENOMIC specimens: {count}')
        for x in self.tol_sample_types:
            count = self.source_collection.count_documents({'sample_type': x + '_specimen'})
            print(f'{x.upper()} specimens: {count}')
        print('\n________________________________________\n')

    # ______________________________________

    # Count the number of samples based on sample type and status
    def get_sample_statistics(self):
        x = self.sample_collection.count_documents({})
        print(f'Total number of samples: {x}\n')

        for t in self.sample_types:
            query = {'sample_type': t}
            count = self.sample_collection.count_documents(query)
            label = self.non_tol_sample_types.get(t,'').upper() if t in self.non_tol_sample_types else t.upper()
            print(f'{label} samples: {count}')
            for s in self.sample_status:
                query['status'] = s
                count = self.sample_collection.count_documents(query)
                print(f'{s.capitalize()} samples: {count}')
            print('___\n')

        print('________________________________________\n')

    # ______________________________________

    # Get distinct items from records
    def get_distinct_items(self):
        # Get number of distinct 'SCIENTIFIC_NAME' or species based on sample collection
        print(f'Number of distinct \'SCIENTIFIC_NAME\' or species for samples:')
        for x in self.tol_sample_types:
            output = self.sample_collection.distinct('SCIENTIFIC_NAME', {'sample_type': x})
            print(f'   {len(output)} distinct {x.upper()} scientific names')

        print('\n________________________________________\n')

    # ______________________________________

    # Custom queries
    # Get number of samples brokered between certain dates
    def get_sample_statistics_between_dates(self):
        # Get number of samples brokered between certain dates
        # Replace the date strings with the desired date range
        # Date period: between April 2017 and March 2023
        d_from_str = '2017-04-01T00:00:00+00:00'  # Earliest possible date e.g.: datetime.min.isoformat()
        d_to_str = '2023-04-01T00:00:00+00:00' # Current UTC datetime e.g.: datetime.now(timezone.utc).isoformat()

        # Validate required date fields
        result = validate_date_from_api(d_from_str, d_to_str)

        # Return error if result is an error
        if isinstance(result, HttpResponse):
            print('Error in date values provided. Please check the date format.')
            return

        # Unpack parsed date values from the result
        d_from_parsed, d_to_parsed = result
        d_from_mm_yyyy = d_from_parsed.strftime('%B %Y')
        d_to_mm_yyyy = d_to_parsed.strftime('%B %Y')

        query = {'time_created': {'$gte': d_from_parsed, '$lt': d_to_parsed}}

        print(f'Number of samples brokered between {d_from_mm_yyyy} and {d_to_mm_yyyy}:')
        for t in self.sample_types:
            query['sample_type'] = t
            count = self.sample_collection.count_documents(query)
            label = self.non_tol_sample_types.get(t,'').upper() if t in self.non_tol_sample_types else t.upper()
            print(f'   {label} sample count: {count}')

        query['sample_type'] = {'$in': self.sample_types}
        count = self.sample_collection.count_documents(query)
        sample_types_str = ', '.join(self.sample_types).replace('isasample', 'genomics').upper()
        print(f'\n   Total number of {sample_types_str} samples: {count}')

    # ______________________________________

    # Group and rank samples by Genomic profile and fetch owner's email address
    ''' 
    NB: This function uses the 'tabulate' library to display the table in the terminal.
        The displayed output can be copied and used in the script, 'convert_tabular_data_to_spreadsheet.py',
        which is located in the 'shared_tools/scripts' directory, to generate an Excel file
    '''
    def rank_genomic_profiles_and_get_owner_email(self):
        print(f'Genomic samples grouped by profile and ranked with owner\'s email:\n')
        pipeline = [
            {'$match': {'type': 'genomics'}},  # Filter for 'genomics' profiles
            {
                '$lookup': {
                    'from': 'SampleCollection',
                    'let': {'profile_id': {'$toString': '$_id'}},  # Convert ObjectId to string
                    'pipeline': [
                        {'$match': {'$expr': {'$eq': ['$profile_id', '$$profile_id']}}}  # Match as string
                    ],
                    'as': 'samples'
                }
            },
            {
                '$addFields': {
                    'sample_count': {'$size': '$samples'}
                }
            },
            {'$sort': {'sample_count': -1}},
            {'$project': {'samples': 0}}
        ]

        genomic_profiles = list(self.profile_collection.aggregate(pipeline))
        user_ids = list(set(profile['user_id'] for profile in genomic_profiles if 'user_id' in profile)) # Extract unique user IDs from profiles
        users = User.objects.filter(id__in=user_ids).values('id', 'email') # Fetch all user emails in a single query
        user_email_map = {user['id']: user['email'] for user in users} # Convert to a dictionary {user_id: email}

        # Define table headers and data
        table_data = []
        table_headers = ['Genomic profile', 'Sample count', 'Owner email address']

        for profile in genomic_profiles:
            profile['owner_email'] = user_email_map.get(profile.get('user_id'), 'Unknown')

        for profile in genomic_profiles:
            # Print the table without library usage
            # print(f"  - Profile: {profile['title']}, {profile['sample_count']} samples, Owner: {profile['owner_email']}")
            # print('\n')
            table_data.append([profile['title'], profile['sample_count'], profile['owner_email']])

        # Print the table using the 'tabulate' library
        print(tabulate(table_data, headers=table_headers, tablefmt='grid'))

        # Uncomment the code below to generate an Excel file from the table data
        # Create a DataFrame from the table data
        # df = pd.DataFrame(table_data, columns=['Profile', 'Sample Count', 'Owner Email'])

        # Write the DataFrame to an Excel file
        # file_path = 'genomic_profiles_statistics_by_rank.xlsx'

        # Check if the file exists and remove it if it does
        # if os.path.exists(file_path):
        # os.remove(file_path)
        # df.to_excel(file_path, index=False)
        # print(f'   Excel file \'{file_path}\' has been created.')

        print('\n________________________________________\n')
