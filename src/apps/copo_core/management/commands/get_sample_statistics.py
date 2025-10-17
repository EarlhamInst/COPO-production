import os
import pandas as pd
import pymongo
import urllib.parse
import common.schemas.utils.data_utils as d_utils

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
        self.stdout.write('\nRunning statistics...')
        self.stdout.write('\n________________________________________\n')

        # Setup MongoDB connection and load data
        self.initialise_db()

        # Get statistics
        self.get_profile_statistics()
        self.get_specimen_statistics()
        self.get_sample_statistics()
        self.get_sample_statistics_by_associated_project()
        self.get_distinct_items()
        self.get_sample_statistics_between_dates()
        self.rank_genomic_profiles_and_get_owner_email()

        # Get ERGA related statistics
        # self.get_sample_statistics_between_dates(sample_type='erga')
        # self.get_sample_statistics(sample_type='erga')
        # self.get_sample_statistics_by_associated_project(sample_type='erga')

    # _______________________

    # MongoDB Connection
    def initialise_db(self):
        username = urllib.parse.quote_plus('copo_user')
        password = urllib.parse.quote_plus('password')
        myclient = pymongo.MongoClient(
            'mongodb://%s:%s@copo_mongo:27017/' % (username, password)
        )
        mydb = myclient['copo_mongo']

        self.profile_collection = mydb['Profiles']
        self.sample_collection = mydb['SampleCollection']
        self.source_collection = mydb['SourceCollection']

        self.profile_types = self.profile_collection.distinct('type')
        self.sample_status = ['accepted', 'pending', 'rejected']
        self.sample_types = self.sample_collection.distinct('sample_type')
        self.non_tol_sample_types = {'isasample': ['genomics', 'biodata']}
        self.non_tol_sample_types_list = list(self.non_tol_sample_types.values())[0]
        self.tol_sample_types = [
            x for x in self.sample_types if x not in self.non_tol_sample_types.keys()
        ]
        self.tol_specimen_types = [x + '_specimen' for x in self.tol_sample_types]

    # ______________________________________

    # Count the number of profile records
    def get_profile_statistics(self):
        print(
            f'\nTotal number of profiles: {self.profile_collection.count_documents({})}'
        )
        for x in self.profile_types:
            print(
                f'   {x.upper()} profiles: {self.profile_collection.count_documents({"type": x})}'
            )

        print('\n________________________________________\n')

    # ______________________________________

    # Count the number of sources/specimens records
    def get_specimen_statistics(self):
        print('Total number of specimens')
        count = self.source_collection.count_documents(
            {'sample_type': {'$nin': self.tol_specimen_types}}
        )
        label = d_utils.join_with_and(
            [item.upper() for item in self.non_tol_sample_types_list]
        )
        print(f'   {label} specimens: {count}')
        for x in self.tol_sample_types:
            count = self.source_collection.count_documents(
                {'sample_type': x + '_specimen'}
            )
            print(f'   {x.upper()} specimens: {count}')
        print('\n________________________________________\n')

    # ______________________________________

    # Count the number of samples based on sample type and associated project
    def get_sample_statistics_by_associated_project(
        self, sample_type=None, associated_projects=None
    ):
        '''
        Aggregates sample counts by normalised associated_tol_project.

        Args:
            associated_projects (list): List of associated tol projects such as ['BGE', 'ERGA_COMMUNITY', 'POP_GENOMICS']
            sample_type (str): Sample type to filter (default: 'erga')

        Returns:
            dict: Aggregated counts and total_count
        '''

        if associated_projects is None:
            if sample_type is None:
                associated_projects = self.profile_collection.distinct(
                    'associated_type'
                )
            else:
                associated_projects = self.profile_collection.distinct(
                    'associated_type', {'type': sample_type}
                )

        if sample_type is None:
            sample_type = 'erga'

        # Step 1: Build regex conditions for any base item + SANGER
        regex_conditions = [
            {
                'associated_tol_project': {
                    '$regex': f'(?=.*{item})(?=.*SANGER)',
                    '$options': 'i',
                }
            }
            for item in associated_projects
        ]

        pipeline = [
            # Step 2: Match documents that are either in the base list OR have base + SANGER
            {
                '$match': {
                    '$and': [{'sample_type': sample_type}],
                    '$or': [
                        {'associated_tol_project': {'$in': associated_projects}},
                        *regex_conditions,
                    ],
                }
            },
            # Step 3: Normalise: map any base+SANGER to base
            {
                '$addFields': {
                    'normalised_associated_tol_project': {
                        '$switch': {
                            'branches': [
                                {
                                    'case': {
                                        '$regexMatch': {
                                            'input': "$associated_tol_project",
                                            'regex': f'(?=.*{item})(?=.*SANGER)',
                                            'options': 'i',
                                        }
                                    },
                                    'then': item,
                                }
                                for item in associated_projects
                            ],
                            'default': "$associated_tol_project",
                        }
                    }
                }
            },
            # Step 4: Group by normalised_associated_tol_project to count
            {
                '$group': {
                    '_id': "$normalised_associated_tol_project",
                    'count': {'$sum': 1},
                }
            },
            {'$sort': {'_id': 1}},
            # Step 5: Aggregate total count
            {
                '$group': {
                    '_id': None,
                    'counts': {'$push': {'type': "$_id", 'count': "$count"}},
                    'total_count': {'$sum': "$count"},
                }
            },
        ]

        result = list(self.sample_collection.aggregate(pipeline))
        if result:
            print(
                f'Sample counts by associated project for {sample_type.upper()} profile:'
            )

            for x in result:
                print(f"   Total: {x['total_count']}\n")
                counts = x['counts']
                for count in counts:
                    print(f"   {count['type']}: {count['count']} samples")
        else:
            print(
                f'No associated project statistics found for {sample_type.upper()} profile'
            )

        print('\n________________________________________\n')

    # ______________________________________

    # Count the number of samples based on sample type and status
    def get_sample_statistics(self, sample_type=None):
        total_samples = self.sample_collection.count_documents({})
        print(f'Total number of samples: {total_samples}\n')

        sample_types = [sample_type] if sample_type else self.sample_types

        for t in sample_types:
            if t not in self.sample_types:
                print(f'Invalid sample type: {t}\n')
                continue

            label = self.non_tol_sample_types.get(t, t)

            # Format label if it's a list
            label = (
                d_utils.join_with_and([item.upper() for item in label])
                if isinstance(label, list)
                else label.upper()
            )

            # Count total for sample type
            query = {'sample_type': t}
            count = self.sample_collection.count_documents(query)
            print(f'   {label} samples: {count}')

            # Count by sample status
            for status in self.sample_status:
                query_with_status = {**query, 'status': status}
                count = self.sample_collection.count_documents(query_with_status)
                print(f'     {status.capitalize()}: {count}')

            print('    ______________________________\n')
        print('________________________________________\n')

    # ______________________________________

    # Get distinct items from records
    def get_distinct_items(self):
        # Get number of distinct 'SCIENTIFIC_NAME' or species based on sample collection
        print(f'Number of distinct \'SCIENTIFIC_NAME\' or species for samples:')
        for x in self.tol_sample_types:
            output = self.sample_collection.distinct(
                'SCIENTIFIC_NAME', {'sample_type': x}
            )
            print(f'   {len(output)} distinct {x.upper()} scientific names')

        print('\n________________________________________\n')

    # ______________________________________

    # Custom queries
    # Get number of samples brokered between certain dates
    def get_sample_statistics_between_dates(self, sample_type=None):
        # Get number of samples brokered between certain dates
        # Replace the date strings with the desired date range
        # Date period: between April 2017 and March 2023
        d_from_str = '2017-04-01T00:00:00+00:00'  # Earliest possible date e.g.: datetime.min.isoformat()
        d_to_str = '2023-04-01T00:00:00+00:00'  # Current UTC datetime e.g.: datetime.now(timezone.utc).isoformat()

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

        print(
            f'Number of samples brokered between {d_from_mm_yyyy} and {d_to_mm_yyyy}:'
        )

        sample_types = [sample_type] if sample_type else self.sample_types

        for t in sample_types:
            query['sample_type'] = t
            count = self.sample_collection.count_documents(query)
            label = self.non_tol_sample_types.get(t, t)

            # Format label if it's a list
            label = (
                d_utils.join_with_and([item.upper() for item in label])
                if isinstance(label, list)
                else label.upper()
            )

            print(f'   {label} samples: {count}')

        # Count total
        if not sample_type:
            query['sample_type'] = {'$in': sample_types}
            total_count = self.sample_collection.count_documents(query)
            sample_types_str = (
                ', '.join(sample_types).replace('isasample', 'genomics/biodata').upper()
            )

            print(f'\n   Total number of {sample_types_str} samples: {total_count}')

        print('\n________________________________________\n')

    # ______________________________________

    # Group and rank samples by Genomics/Biodata profile and fetch owner's email address
    ''' 
    NB: This function uses the 'tabulate' library to display the table in the terminal.
        The displayed output can be copied and used in the script, 'convert_tabular_data_to_spreadsheet.py',
        which is located in the 'shared_tools/scripts' directory, to generate an Excel file
    '''

    def rank_genomic_profiles_and_get_owner_email(self):
        label = d_utils.join_with_and(
            [item.title() for item in self.non_tol_sample_types_list]
        )
        print(f'{label} samples grouped by profile and ranked with owner\'s email:\n')
        pipeline = [
            {
                '$match': {'type': {'$in': self.non_tol_sample_types_list}}
            },  # Filter for 'genomics/biodata' profiles
            {
                '$lookup': {
                    'from': 'SampleCollection',
                    'let': {
                        'profile_id': {'$toString': '$_id'}
                    },  # Convert ObjectId to string
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {'$eq': ['$profile_id', '$$profile_id']}
                            }
                        }  # Match as string
                    ],
                    'as': 'samples',
                }
            },
            {'$addFields': {'sample_count': {'$size': '$samples'}}},
            {'$sort': {'sample_count': -1}},
            {'$project': {'samples': 0}},
        ]

        genomic_profiles = list(self.profile_collection.aggregate(pipeline))
        user_ids = list(
            set(
                profile['user_id']
                for profile in genomic_profiles
                if 'user_id' in profile
            )
        )  # Extract unique user IDs from profiles
        users = User.objects.filter(id__in=user_ids).values(
            'id', 'email'
        )  # Fetch all user emails in a single query
        user_email_map = {
            user['id']: user['email'] for user in users
        }  # Convert to a dictionary {user_id: email}

        # Define table headers and data
        table_data = []
        table_headers = [
            'Genomics/Biodata profile',
            'Sample count',
            'Owner email address',
        ]

        for profile in genomic_profiles:
            profile['owner_email'] = user_email_map.get(
                profile.get('user_id'), 'Unknown'
            )

        for profile in genomic_profiles:
            # Print the table without library usage
            # print(f"  - Profile: {profile['title']}, {profile['sample_count']} samples, Owner: {profile['owner_email']}")
            # print('\n')
            table_data.append(
                [profile['title'], profile['sample_count'], profile['owner_email']]
            )

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
