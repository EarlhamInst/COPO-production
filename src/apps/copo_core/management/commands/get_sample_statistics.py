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

'''
Purpose: To fetch statistics of records in COPO.
To run the script: $ python shared_tools/scripts/get_sample_statistics.py

Alternatively, to execute the script via VSCode configuration, set the following in `launch.json` file:
{
    "name": "Python: Get sample statistics",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}/manage.py",
    "env": {
    "PYTHONPATH": "${workspaceFolder}/lib:${PYTHONPATH}"
    },
    "args": ["get_sample_statistics"],
    "django": true,
    "justMyCode": false
}
________________________________________
    
To call an individual function from this Django command, get_sample_statistics,
execute the following in the terminal:
# Open Django interactive shell
$ python manage.py shell

# Import the Command class
from src.apps.copo_core.management.commands.get_sample_statistics import Command

# Allow breakpoints. Ensure that the function to be called has breakpoints set
import pdb; pdb.set_trace()

cmd = Command() # Instantiate the Command class
cmd.initialise_db() # Initialise the database

# Call any method inside the class
# e.g. cmd.rank_users_by_samples_and_data_files_submitted()
cmd.my_function(args_here) 
________________________________________

To clear terminal in Python interactive shell, use the following command:
import os
os.system('cls' if os.name == 'nt' else 'clear')
'''


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

        # self.rank_genomic_profiles_and_get_owner_email()
        # self.rank_users_by_samples_and_data_files_submitted(
        #     start_from='samples', max_users=10
        # )
        # self.rank_users_by_samples_and_data_files_submitted(
        #     start_from='data_files', max_users=10
        # )
        ## Get only email address of users linked to profiles
        # self.get_email_addresses_of_registered_users()
        ## Get all users' email addresses
        # self.get_email_addresses_of_registered_users(only_with_profiles=False)
        self.get_average_samples_submitted_per_user()

        # ERGA related statistics
        # self.get_sample_statistics_between_dates(sample_type='erga')
        # self.get_sample_statistics(sample_type='erga')
        # self.get_sample_statistics_by_associated_project(sample_type='erga')

    # _______________________

    # MongoDB Connection
    def initialise_db(self):
        username = urllib.parse.quote_plus('copo_user')
        password = urllib.parse.quote_plus('password')

        try:
            mongodb_client = pymongo.MongoClient(
                'mongodb://%s:%s@copo_mongo:27017/' % (username, password)
            )
            # Attempt an operation to trigger authentication
            mongodb_client.admin.command('ping')
        except pymongo.errors.OperationFailure as e:
            # Raised when authentication fails
            raise PermissionError(f"MongoDB authentication failed: {e}")
        except pymongo.errors.ServerSelectionTimeoutError as e:
            # Raised if server cannot be reached
            raise ConnectionError(f"Cannot connect to MongoDB: {e}")

        database = mongodb_client['copo_mongo']

        self.profile_collection = database['Profiles']
        self.sample_collection = database['SampleCollection']
        self.source_collection = database['SourceCollection']
        self.ena_file_collection = database['EnaFileTransferCollection']

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

        # Match BIOBLITZ | BGE → BIOBLITZ
        regex_conditions.append(
            {
                'associated_tol_project': {
                    '$regex': '(?=.*BIOBLITZ)(?=.*BGE)',
                    '$options': 'i',
                }
            }
        )

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
                                # SANGER mapping
                                *[
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
                                # BIOBLITZ | BGE mapping
                                {
                                    'case': {
                                        '$regexMatch': {
                                            'input': "$associated_tol_project",
                                            'regex': '(?=.*BIOBLITZ)(?=.*BGE)',
                                            'options': 'i',
                                        }
                                    },
                                    'then': "BIOBLITZ",
                                },
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
        COPO_START_DATE = '2014-09-14T00:00:00+00:00'
        CURRENT_DATE = datetime.now(timezone.utc).isoformat()  # Current UTC datetime

        # Earliest possible date e.g.: datetime.min.isoformat()
        d_from_str = COPO_START_DATE  # '2017-04-01T00:00:00+00:00'

        # Current UTC datetime e.g.: datetime.now(timezone.utc).isoformat()
        d_to_str = CURRENT_DATE  # '2023-04-01T00:00:00+00:00'

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
    def rank_genomic_profiles_and_get_owner_email(self):
        ''' 
        NB: This function uses the 'tabulate' library to display the table in the terminal.
            The displayed output can be copied and used in the script, 'convert_tabular_data_to_spreadsheet.py',
            which is located in the 'shared_tools/scripts' directory, to generate an Excel file
        '''
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

    # ______________________________________

    # Rank users by submitted samples and/ data files
    def rank_users_by_samples_and_data_files_submitted(
        self, start_from='samples', max_users=10
    ):
        '''
        :param start_from: 'samples' or 'data_files' — defines the primary metric for ranking
        
        NB: This function uses the 'tabulate' library to display the table in the terminal.
            The displayed output can be copied and used in the script, 'convert_tabular_data_to_spreadsheet.py',
            which is located in the 'shared_tools/scripts' directory, to generate an Excel file
        '''
        if start_from not in ('samples', 'data_files'):
            raise ValueError("'start_from' field must be 'samples' or 'data_files'")

        # Define base collection which can be either be 'SampleCollection'
        # or 'EnaFileTransferCollection' depending on the starting point of the ranking.
        sort_by = {}
        projection = {'_id': 1}
        table_header_map = {
            'User ID':'_id',
            'First name': 'first_name',
            'Last name': 'last_name',
            'Email address': 'email'
        }

        if start_from == 'samples':
            base_collection = self.sample_collection
            primary_field = 'sample_count'
            secondary_field = 'data_file_count'
            # Sort by sample_count in descending order,
            # then by data_file_count in descending order
            sort_by = {primary_field: -1, secondary_field: -1}  
            projection['sample_count'] = 1
            projection['data_file_count'] = 1
            table_header_map.update(
                {'Sample count': 'sample_count', 'Data file count': 'data_file_count'}
            )
        else:
            base_collection = self.ena_file_collection
            primary_field = 'data_file_count'
            sort_by = {primary_field: -1}
            projection['data_file_count'] = 1
            table_header_map['Data file count'] = 'data_file_count'

        pipeline = []

        if start_from == 'samples':
            # Default logic: start from SampleCollection
            pipeline.extend(
                [
                    {
                        '$lookup': {
                            'from': 'Profiles',
                            # SampleCollection.profile_id
                            'let': {'pid': '$profile_id'},
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            # Profiles._id
                                            '$eq': [
                                                '$_id',
                                                {'$toObjectId': '$$pid'},
                                            ]
                                        }
                                    }
                                }
                            ],
                            'as': 'profile_doc',
                        }
                    },
                    # Unwind profile_doc to access user_id field
                    {'$unwind': '$profile_doc'},
                    # Only documents with valid profiles and with 'accepted' i.e. submitted samples will proceed
                    # This ensures that samples with associated profiles are counted for.
                    {'$match': {'profile_doc': {'$ne': None}, 'status': 'accepted'}},
                    # Group samples by user_id within the profile document
                    {
                        '$group': {
                            '_id': '$profile_doc.user_id',
                            'profile_ids': {'$addToSet': '$profile_doc._id'},
                            'sample_count': {'$sum': 1},
                            'data_file_count': {
                                '$sum': {'$ifNull': ['$data_files_count', 0]}
                            },
                        }
                    },
                    # Lookup data files in the collection, EnaFileTransferCollection, per profile_id
                    {
                        '$lookup': {
                            'from': 'EnaFileTransferCollection',
                            'let': {'pids': '$profile_ids'},
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$in': [
                                                        '$profile_id',
                                                        {
                                                            '$map': {
                                                                'input': '$$pids',
                                                                'as': 'pid',
                                                                'in': {
                                                                    '$toString': '$$pid'
                                                                },
                                                            }
                                                        },
                                                    ]
                                                },
                                                # For 'is_archived' field, '0' means not archived, '1' means archived.
                                                # Submitted data files that have been successfully transferred to ENA are considered as archived data files.
                                                # {'$eq': ['$is_archived', '1']},
                                                # For 'status' field, these are the possible values: 'pending', 'processing', 'complete' and 'ena_complete'
                                                {'$eq': ['$status', 'ena_complete']},
                                                # For 'transfer_status' field, these are the possible values:
                                                # 2 which means get file from minio, 3 which means NIL, 4 which means NIL and 5 which means transfer file to ENA
                                                {'$eq': ['$transfer_status', 5]},
                                            ]
                                        }
                                    }
                                },
                                {'$count': 'file_count'},
                            ],
                            'as': 'ena_files',
                        }
                    },
                    # Flatten the data file count
                    {
                        '$addFields': {
                            'data_file_count': {
                                '$ifNull': [
                                    {'$arrayElemAt': ['$ena_files.file_count', 0]},
                                    0,
                                ]
                            }
                        }
                    },
                ]
            )
        else:
            # Reverse logic: start from EnaFileTransferCollection
            pipeline.extend(
                [
                    {
                        '$match': {
                            # For 'is_archived' field, '0' means not archived, '1' means archived.
                            # Submitted data files that have been successfully transferred to ENA are considered as archived data files.
                            # 'is_archived': '1',
                            # For 'status' field, these are the possible values: 'pending', 'processing', 'complete' and 'ena_complete'
                            'status': 'ena_complete',
                            # For 'transfer_status' field, these are the possible values:
                            # 2 which means get file from minio, 3 which means NIL, 4 which means NIL and 5 which means transfer file to ENA
                            'transfer_status': 5,
                        }
                    },
                    {
                        '$lookup': {
                            'from': 'Profiles',
                            # EnaFileTransferCollection.profile_id
                            'let': {'pid': '$profile_id'},
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            # Profiles._id
                                            '$eq': ['$_id', {'$toObjectId': '$$pid'}]
                                        }
                                    }
                                }
                            ],
                            'as': 'profile_doc',
                        }
                    },
                    # Unwind profile_doc to access user_id field
                    {'$unwind': '$profile_doc'},
                    # Only documents with valid profiles will proceed. This ensures that data
                    # files with associated profiles are counted for.
                    {'$match': {'profile_doc': {'$ne': None}}},
                    # Group by user_id and count ENA files
                    {
                        '$group': {
                            '_id': '$profile_doc.user_id',
                            'profile_ids': {'$addToSet': '$profile_doc._id'},
                            'data_file_count': {'$sum': 1},
                        }
                    },
                ]
            )

        # Sort, limit, project
        pipeline.extend(
            [
                # Sort by number of samples and data files submitted in descending order
                {'$sort': sort_by},
                # Limit to what is set as max_users (default: 10)
                {'$limit': max_users},
                # Project fields
                {'$project': projection},
            ]
        )

        # Execute the MongoDB aggregation pipeline
        users_with_samples = list(base_collection.aggregate(pipeline))

        # Get details of the ranked users
        user_ids = [x['_id'] for x in users_with_samples]
        users = User.objects.filter(id__in=user_ids).values(
            'id', 'first_name', 'last_name', 'email'
        )
        user_map = {x['id']: x for x in users}

        for x in users_with_samples:
            user_info = user_map.get(x['_id'], {})
            x.update(user_info)

        # Define table headers and data
        table_data = []

        for user in users_with_samples:
            row = []
            for key in table_header_map.values():
                value = user.get(key, '')
                # Convert '_id' to string
                if key == '_id':
                    value = str(value)  
                row.append(value)
            table_data.append(row)

        print(
            f"\nTop {max_users} users ranked by {primary_field.replace('_', ' ')}:\n"
        )

        # Print the table using the 'tabulate' library
        table_headers = list(table_header_map.keys())

        print(tabulate(table_data, headers=table_headers, tablefmt='grid'))

        # Uncomment the code below to generate an Excel file from the table data
        # Create a DataFrame from the table data
        # df = pd.DataFrame(table_data, columns=table_headers)

        # Write the DataFrame to an Excel file
        # file_path = f'top_{max_users}_users_rank_by_{primary_field}.xlsx'

        # Check if the file exists and remove it if it does
        # if os.path.exists(file_path):
        # os.remove(file_path)
        # df.to_excel(file_path, index=False, sheet_name=f"Top {max_users} users ranked by {primary_field.replace('_', ' ')}"")
        # print(
        #     f"\n   Excel file '{file_path}' has been created in '{os.getcwd()}' directory."
        # )

        print('\n________________________________________\n')

    # ______________________________________

    # Get a list of registered users' email address
    def get_email_addresses_of_registered_users(self,  only_with_profiles=True):
        ''' 
        NB: This function uses the 'tabulate' library to display the table in the terminal.
            The displayed output can be copied and used in the script, 'convert_tabular_data_to_spreadsheet.py',
            which is located in the 'shared_tools/scripts' directory, to generate an Excel file
        '''
        if only_with_profiles:
            msg = ' email addresses of registered users linked to profiles'
            file_path_suffix = (
                'copo_registered_users_with_profiles_email_addresses.xlsx'
            )
            sheet_name = 'Registered users linked to profiles email addresses'
            user_ids = self.profile_collection.distinct('user_id')
            users = User.objects.filter(id__in=user_ids).values('id', 'email')
        else:
            msg = ' email addresses of all registered users'
            file_path_suffix = 'copo_registered_users_email_addresses.xlsx'
            sheet_name = 'All registered users email addresses'
            users = User.objects.all().values('id', 'email')

        # Convert to a dictionary e.g. {user_id: email_address}
        user_email_map = {
            user['id']: user['email'] for user in users
        }  
        # Define table headers and data
        table_data = []
        table_headers = ['User ID', 'Email address']

        # Identify user IDs with no email address
        users_with_no_email = [user_id for user_id, email in user_email_map.items() if not email]

        # Only include users with email addresses in the table data
        for user_id, email in user_email_map.items():
            if email:
                table_data.append([str(user_id), email])

        users_with_email_count = len(table_data)
        print(f'\n{users_with_email_count}{msg}:\n')

        # Print the table using the 'tabulate' library
        # Table: Users with email address
        print(tabulate(table_data, headers=table_headers, tablefmt='grid'))

        # Table: Users with no email address
        if users_with_no_email:
            msg_suffix = 'linked to profiles' if only_with_profiles else 'in the system'
            print(f"\nWarning: {len(users_with_no_email)} user IDs have no email address {msg_suffix}:\n")
            print(tabulate([[str(user_id)] for user_id in users_with_no_email], headers=['User ID'], tablefmt='grid'))

        # Uncomment the code below to generate an Excel file from the table data
        # Create a DataFrame from the table data
        # df = pd.DataFrame(table_data, columns=table_headers)

        # Write the DataFrame to an Excel file
        # file_path = f'{users_with_email_count}_{file_path_suffix}'

        # Check if the file exists and remove it if it does
        # if os.path.exists(file_path):
        #     os.remove(file_path)
        # df.to_excel(file_path, index=False, sheet_name=f'{users_with_email_count} {sheet_name}')
        # print(f"\n   Excel file '{file_path}' has been created in '{os.getcwd()}' directory.")

        print('\n________________________________________\n')

    def get_average_samples_submitted_per_user(self):
        print('\nAverage number of samples submitted per user:\n')
        pipeline = [
            # Only get samples that have been accepted i.e. been submitted already
            {'$match': {'status': 'accepted'}},
            # Group by 'created_by' which is the email address of
            # the person who submitted the samples
            {
                '$group': {
                    '_id': {'$toLower': '$created_by'},
                    'sample_count': {'$sum': 1},
                }
            },
            # Compute average across users
            {
                '$group': {
                    '_id': None,
                    'average_samples_per_user': {'$avg': '$sample_count'},
                    'total_users': {'$sum': 1},
                }
            },
        ]

        result = list(self.sample_collection.aggregate(pipeline))
        if result:
            average_samples = result[0]['average_samples_per_user']
            print(f'   Average samples submitted per user: {average_samples:.2f}')
        else:
            print('   No sample data found to calculate average.')

        print('\n________________________________________\n')
