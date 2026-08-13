import os
import pandas as pd
import pymongo
import urllib.parse

from datetime import datetime, timezone
from django.contrib.auth.models import User
from django.core.management import BaseCommand
from django.http import HttpResponse
from tabulate import tabulate

from common.schemas.utils.data_utils import join_with_and
from src.apps.api.utils import validate_date_from_api

'''
Purpose: To fetch statistics of records in COPO.
To run the script: $ python manage.py get_statistics

Alternatively, to execute the script via VSCode configuration, set the following in `launch.json` file:
{
    "name": "Python: Get statistics",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}/manage.py",
    "env": {
    "PYTHONPATH": "${workspaceFolder}/lib:${PYTHONPATH}"
    },
    "args": ["get_statistics"],
    "django": true,
    "justMyCode": false
}
________________________________________

To copy file from VM to local machine, execute the following in the terminal:
$ ssh -i <ssh-key.pem> <vm-username>@<vm-name-or-ip-address> "sudo docker exec <container-id> cat <path-to-file-in-container-.ext>" > <path-for-file-on-local-machine-.ext>

________________________________________
    
To call an individual function from this Django command, get_statistics,
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


class MongoDB:
    def __init__(self):
        # Access the database
        mongodb_client = self.initialise_database()
        database = mongodb_client['copo_mongo']

        # Database collections
        self.profile_collection = database['Profiles']
        self.sample_collection = database['SampleCollection']
        self.source_collection = database['SourceCollection']
        self.ena_file_collection = database['EnaFileTransferCollection']
        self.single_cell_collection = database['SingleCellCollection']

        # Date filters
        # NB: Replace the date strings with the desired date range
        # e.g. Date period: between April 2017 and March 2023
        # COPO_START_DATE == d_from, e.g. # '2017-04-01T00:00:00+00:00'
        self.COPO_START_DATE = '2017-04-01T00:00:00+00:00'
        # CURRENT_DATE == Current UTC datetime == d_to e.g. '2023-04-01T00:00:00+00:00'
        self.CURRENT_DATE = datetime.now(timezone.utc).isoformat()

        # Declarations
        self.profile_types = self.profile_collection.distinct('type')

        self.sample_status = ['accepted', 'pending', 'rejected']
        self.sample_types = self.sample_collection.distinct('sample_type')
        self.non_tol_sample_types = {'isasample': ['genomics', 'biodata']}
        self.non_tol_sample_types_list = list(self.non_tol_sample_types.values())[0]
        self.tol_sample_types = [
            x for x in self.sample_types if x not in self.non_tol_sample_types.keys()
        ]
        self.tol_specimen_types = [x + '_specimen' for x in self.tol_sample_types]

        self.single_cell_checklist_types = self.single_cell_collection.distinct(
            'schema_name'
        )
        self.single_cell_status = ['PUBLIC', 'PRIVATE', 'pending']
        self.submission_repositories = ['ena', 'zenodo']

        # Limit the number of items displayed in the output
        self.max_items_to_display = 3

    # ______________________________________

    def initialise_database(self):
        # Connect to MongoDB
        username = urllib.parse.quote_plus('copo_user')
        password = urllib.parse.quote_plus('password')

        try:
            mongodb_client = pymongo.MongoClient(
                'mongodb://%s:%s@copo_mongo:27017/' % (username, password)
            )
            # Attempt an operation to trigger authentication
            mongodb_client.admin.command('ping')
            return mongodb_client
        except pymongo.errors.OperationFailure as e:
            # Raised when authentication fails
            raise PermissionError(f'MongoDB authentication failed: {e}')
        except pymongo.errors.ServerSelectionTimeoutError as e:
            # Raised if server cannot be reached
            raise ConnectionError(f'Cannot connect to MongoDB: {e}')

    # ______________________________________

    # Date filtering function to be used in MongoDB queries
    def build_date_filter(self, apply_date_filter, d_from=None, d_to=None):
        if not apply_date_filter:
            return {}, ''

        if d_from is None:
            d_from = self.COPO_START_DATE

        if d_to is None:
            d_to = self.CURRENT_DATE

        # Earliest possible date e.g.: datetime.min.isoformat()
        d_from_str = d_from  # '2017-04-01T00:00:00+00:00' # COPO_START_DATE

        # Current UTC datetime e.g.: datetime.now(timezone.utc).isoformat()
        d_to_str = d_to  # '2023-04-01T00:00:00+00:00' # CURRENT_DATE

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

        return (
            {'time_created': {'$gte': d_from_parsed, '$lt': d_to_parsed}},
            f'between {d_from_mm_yyyy} and {d_to_mm_yyyy}',
        )


class ProfileStatistics(MongoDB):
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

    # Group and rank samples by Genomics/Biodata profile and fetch owner's email address
    def rank_genomic_profiles_and_get_owner_email(self):
        '''
        NB: This function uses the 'tabulate' library to display the table in the terminal.
            The displayed output can be copied and used in the script, 'convert_tabular_data_to_spreadsheet.py',
            which is located in the 'shared_tools/scripts' directory, to generate an Excel file
        '''
        label = join_with_and([item.title() for item in self.non_tol_sample_types_list])
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


class SampleStatistics(MongoDB):
    # Count the number of samples based on sample type and associated project
    def get_sample_statistics_by_associated_project(
        self, sample_type=None, associated_projects=None, apply_date_filter=False
    ):
        '''
        Aggregates sample counts by normalised associated_tol_project.

        Args:
            associated_projects (list): List of associated tol projects such as ['BGE', 'ERGA_COMMUNITY', 'POP_GENOMICS']
            sample_type (str): Sample type to filter (default: 'erga')

        Returns:
            dict: Aggregated counts and total_count
        '''
        # Build date filter and get the date period for display
        date_filter_query, date_period = self.build_date_filter(
            apply_date_filter=apply_date_filter
        )
        suffix = f' {date_period}' if apply_date_filter else ''

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
                    '$and': [
                        {
                            'sample_type': sample_type,
                            **date_filter_query,
                        }
                    ],
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
                                                'input': '$associated_tol_project',
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
                                            'input': '$associated_tol_project',
                                            'regex': '(?=.*BIOBLITZ)(?=.*BGE)',
                                            'options': 'i',
                                        }
                                    },
                                    'then': 'BIOBLITZ',
                                },
                            ],
                            'default': '$associated_tol_project',
                        }
                    }
                }
            },
            # Step 4: Group by normalised_associated_tol_project to count
            {
                '$group': {
                    '_id': '$normalised_associated_tol_project',
                    'count': {'$sum': 1},
                }
            },
            {'$sort': {'_id': 1}},
            # Step 5: Aggregate total count
            {
                '$group': {
                    '_id': None,
                    'counts': {'$push': {'type': '$_id', 'count': '$count'}},
                    'total_count': {'$sum': '$count'},
                }
            },
        ]

        result = list(self.sample_collection.aggregate(pipeline))
        if result:
            print(
                f'Sample counts by associated project for {sample_type.upper()} profile{suffix}:'
            )

            for x in result:
                print(f"   Total: {x['total_count']}\n")
                counts = x['counts']
                for count in counts:
                    print(f"   {count['type']}: {count['count']} samples")
        else:
            print(
                f'No associated project statistics found for {sample_type.upper()} profile{suffix}'
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
                join_with_and([item.upper() for item in label])
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
    def get_distinct_scientific_names(self, apply_date_filter=False):
        '''
        Returns a count of distinct 'SCIENTIFIC_NAME' or species for each sample type

        # NB: 'SCIENTIFIC_NAME' can be found in either 'SCIENTIFIC_NAME' or
        # 'species_list.SCIENTIFIC_NAME' fields in the sample collection
        '''
        # Build date filter and get the date period for display
        date_filter_query, date_period = self.build_date_filter(
            apply_date_filter=apply_date_filter
        )
        suffix = f' {date_period}' if apply_date_filter else ''

        print(
            f'\nNumber of distinct \'SCIENTIFIC_NAME\' or species for samples{suffix}:'
        )
        for x in self.tol_sample_types:
            output1 = self.sample_collection.distinct(
                'SCIENTIFIC_NAME',
                {
                    'sample_type': x,
                    **date_filter_query,
                },
            )
            output2 = self.sample_collection.distinct(
                'species_list.SCIENTIFIC_NAME',
                {
                    'sample_type': x,
                    **date_filter_query,
                },
            )

            # Merge the two lists and remove 'None' values
            output = (set(output1) | set(output2)) - {None}

            print(f'   {len(output)} distinct {x.upper()} scientific names')

        print('\n________________________________________\n')

    # ______________________________________

    # Get distinct items from records by GAL
    def get_distinct_scientific_names_grouped_by_gal(self, apply_date_filter=False):
        '''
        Returns the number of distinct 'SCIENTIFIC_NAME' or species grouped by
        Genome Acquisition Lab (GAL) from the sample collection
        '''
        # Build date filter and get the date period for display
        date_filter_query, date_period = self.build_date_filter(
            apply_date_filter=apply_date_filter
        )
        suffix = f' {date_period}' if apply_date_filter else ''

        print(
            f'Number of distinct "SCIENTIFIC_NAME" or species grouped by GAL for samples{suffix}:'
        )

        for x in self.tol_sample_types:
            pipeline = [
                {
                    '$match': {
                        'sample_type': x,
                        **date_filter_query,
                    }
                },
                {
                    '$group': {
                        '_id': {'$ifNull': ['$GAL', '$PARTNER']},
                        'scientific_names': {
                            '$addToSet': {
                                '$concatArrays': [
                                    {
                                        '$cond': [
                                            {
                                                '$isArray': '$species_list.SCIENTIFIC_NAME'
                                            },
                                            '$species_list.SCIENTIFIC_NAME',
                                            [],
                                        ]
                                    },
                                    {
                                        '$cond': [
                                            {'$ne': ['$SCIENTIFIC_NAME', None]},
                                            ['$SCIENTIFIC_NAME'],
                                            [],
                                        ]
                                    },
                                ]
                            }
                        },
                    }
                },
                {
                    '$project': {
                        '_id': 0,
                        'GAL': '$_id',
                        'scientific_names': {
                            '$reduce': {
                                'input': '$scientific_names',
                                'initialValue': [],
                                'in': {'$setUnion': ['$$value', '$$this']},
                            }
                        },
                    }
                },
                {
                    '$project': {
                        'GAL': 1,
                        'scientific_names': {
                            '$slice': [
                                '$scientific_names',
                                self.max_items_to_display,
                            ]
                        },
                        'count': {'$size': '$scientific_names'},
                    }
                },
            ]

            output = list(self.sample_collection.aggregate(pipeline))
            total_count = sum(item['count'] for item in output)

            print(
                f'\n{x.upper()} sample type ({total_count} distinct scientific names):'
            )
            for row in output:
                print(
                    f"   {row['GAL']} GAL/PARTNER: {row['count']} distinct scientific names like {join_with_and(row['scientific_names'])}"
                )

        print('\n________________________________________\n')

    # ______________________________________

    # Custom queries
    # Get number of samples brokered between certain dates
    def get_sample_statistics_between_dates(
        self, sample_type=None, apply_date_filter=True
    ):
        # Get number of samples brokered between certain dates

        # Build date filter and get the date period for display
        date_filter_query, date_period = self.build_date_filter(
            apply_date_filter=apply_date_filter
        )
        suffix = f' {date_period}' if apply_date_filter else ''

        print(f'Number of samples brokered{suffix}:')

        sample_types = [sample_type] if sample_type else self.sample_types
        query = date_filter_query if apply_date_filter else {}

        for t in sample_types:
            query['sample_type'] = t
            count = self.sample_collection.count_documents(query)
            label = self.non_tol_sample_types.get(t, t)

            # Format label if it's a list
            label = (
                join_with_and([item.upper() for item in label])
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

    # Get count of sequencing centres from profiles used in sample submissions between certain dates
    def get_sequencing_centres_used_for_sample_submission(
        self, sample_type='erga', apply_date_filter=False
    ):
        pipeline = []
        suffix = ''
        sample_types = [sample_type] if sample_type else self.tol_sample_types

        # Build date filter and get the date period for display
        sample_date_filter_query, date_period = self.build_date_filter(
            apply_date_filter=apply_date_filter
        )
        # Replace 'time_created' with 'date_created' and 'date_modified' for the filter
        if apply_date_filter:
            suffix = f' {date_period}'
            profile_date_filter_query = {
                '$or': [
                    {'date_created': sample_date_filter_query['time_created']},
                    {'date_modified': sample_date_filter_query['time_created']},
                ]
            }
            pipeline.append({'$match': profile_date_filter_query})

        pipeline.extend(
            [
                {
                    '$lookup': {
                        'from': 'SampleCollection',
                        'let': {'profile_id': {'$toString': '$_id'}},
                        'pipeline': [
                            {
                                '$match': {
                                    '$expr': {'$eq': ['$profile_id', '$$profile_id']},
                                    'sample_type': {'$in': sample_types},
                                    **sample_date_filter_query,  # sample date filter
                                }
                            },
                            {
                                '$project': {
                                    '_id': 0,
                                    'sample_type': 1,
                                }
                            },
                        ],
                        'as': 'matching_samples',
                    }
                },
                # Retain profiles that have matching samples
                {'$match': {'matching_samples': {'$ne': []}}},
                {'$unwind': '$matching_samples'},
                {'$unwind': '$sequencing_centre'},
                # Get unique combinations of sample_type + sequencing_centre
                {
                    '$group': {
                        '_id': {
                            'sample_type': '$matching_samples.sample_type',
                            'sequencing_centre': '$sequencing_centre',
                        }
                    }
                },
                # Create overall and results per sample type
                {
                    '$facet': {
                        'overall': [
                            {
                                '$group': {
                                    '_id': None,
                                    'sequencing_centres': {
                                        '$addToSet': '$_id.sequencing_centre'
                                    },
                                }
                            },
                            {
                                '$project': {
                                    '_id': 0,
                                    'distinct_sequencing_centres_count': {
                                        '$size': '$sequencing_centres'
                                    },
                                    'sequencing_centres': 1,
                                }
                            },
                        ],
                        'by_sample_type': [
                            {
                                '$group': {
                                    '_id': '$_id.sample_type',
                                    'sequencing_centres': {
                                        '$addToSet': '$_id.sequencing_centre'
                                    },
                                }
                            },
                            {
                                '$project': {
                                    '_id': 0,
                                    'sample_type': '$_id',
                                    'distinct_sequencing_centres_count': {
                                        '$size': '$sequencing_centres'
                                    },
                                    'sequencing_centres': 1,
                                }
                            },
                        ],
                    }
                },
            ]
        )

        result = list(self.profile_collection.aggregate(pipeline))

        print('\nCount of distinct sequencing centres used in sample submissions:\n')
        if result:
            data = result[0]

            # Overall count
            if data.get('overall'):
                distinct_centres_count = data['overall'][0][
                    'distinct_sequencing_centres_count'
                ]

                print(
                    f'   Overall: {distinct_centres_count} distinct sequencing centres{suffix}\n'
                )

            # Count by sample type
            for sample_result in data.get('by_sample_type', []):
                sample_type_str = sample_result['sample_type']
                distinct_centres_count = sample_result[
                    'distinct_sequencing_centres_count'
                ]
                sequencing_centres = sample_result['sequencing_centres']

                print(
                    f'      - {sample_type_str.upper()}: {distinct_centres_count} distinct sequencing centres\n'
                    f'\t\t({join_with_and(sequencing_centres)})'
                )
        else:
            print(f'   No profiles found to calculate distinct sequencing centres')

        print('\n________________________________________\n')

    # ______________________________________

    # Get count of GAL/PARTNER used for sample submission between certain dates
    def get_gal_partner_used_for_sample_submission(
        self, sample_type=None, apply_date_filter=False
    ):
        sample_types = self.tol_sample_types if sample_type is None else [sample_type]

        # Build date filter and get the date period for display
        date_filter_query, date_period = self.build_date_filter(
            apply_date_filter=apply_date_filter
        )
        suffix = f' {date_period}' if apply_date_filter else ''

        pipeline = [
            {
                '$match': {
                    'sample_type': {'$in': sample_types},
                    **date_filter_query,
                }
            },
            {
                '$facet': {
                    'overall': [
                        {
                            '$group': {
                                '_id': None,
                                'total_samples': {'$sum': 1},
                                'GALs': {'$addToSet': '$GAL'},
                                'PARTNERs': {'$addToSet': '$PARTNER'},
                            }
                        },
                        {
                            '$project': {
                                '_id': 0,
                                'total_samples': 1,
                                'total_GALs': {'$size': '$GALs'},
                                'total_PARTNERs': {'$size': '$PARTNERs'},
                            }
                        },
                    ],
                    'by_sample_type': [
                        {
                            '$group': {
                                '_id': '$sample_type',
                                'total_samples': {'$sum': 1},
                                'GALs': {'$addToSet': '$GAL'},
                                'PARTNERs': {'$addToSet': '$PARTNER'},
                            }
                        },
                        {
                            '$project': {
                                '_id': 0,
                                'sample_type': '$_id',
                                'total_samples': 1,
                                'total_GALs': {'$size': '$GALs'},
                                'total_PARTNERs': {'$size': '$PARTNERs'},
                                'GALs': 1,
                                'PARTNERs': 1,
                            }
                        },
                    ],
                }
            },
        ]

        result = list(self.sample_collection.aggregate(pipeline))

        print(
            f'\nCount of distinct Genome Acquisition Laboratory (GAL)/PARTNER used for sample submission{suffix}:\n'
        )

        if result:
            data = result[0]

            # Overall results
            overall = data['overall'][0] if data['overall'] else {}

            total_samples = overall.get('total_samples', 0)
            distinct_GALs_count = overall.get('total_GALs', 0)
            distinct_PARTNERs_count = overall.get('total_PARTNERs', 0)

            print(
                f'   Overall: {total_samples} samples, '
                f'{distinct_GALs_count} distinct GALs and '
                f'{distinct_PARTNERs_count} distinct PARTNERs used{suffix}\n'
            )

            # Results by sample type
            for sample_result in data['by_sample_type']:
                sample_type_str = sample_result['sample_type']
                total_samples = sample_result['total_samples']
                distinct_GALs_count = sample_result['total_GALs']
                distinct_PARTNERs_count = sample_result['total_PARTNERs']

                print(
                    f'      {sample_type_str.upper()}: {total_samples} samples, '
                    f'{distinct_GALs_count} distinct GAL and '
                    f'{distinct_PARTNERs_count} distinct PARTNER'
                )

                print(f"         - GAL: {join_with_and(sample_result['GALs']) or None}")
                print(
                    f"         - PARTNER: {join_with_and(sample_result['PARTNERs']) or None}\n"
                )
        else:
            print(f'   No sample data found{suffix}')

        print('\n________________________________________\n')

    # ______________________________________


class SingleCellStatistics(MongoDB):

    def get_single_cell_statistics(self, schema_type=None, apply_date_filter=False):
        single_cell_date_filter_query = {}
        suffix = ''
        schema_types = (
            [schema_type] if schema_type else self.single_cell_checklist_types
        )

        # Build date filter and get the date period for display
        sample_date_filter_query, date_period = self.build_date_filter(
            apply_date_filter=apply_date_filter
        )
        # Replace 'time_created' with 'date_created' and 'date_modified' for the filter
        if apply_date_filter:
            suffix = f' {date_period}'
            single_cell_date_filter_query = {
                '$or': [
                    {'date_created': sample_date_filter_query['time_created']},
                    {'date_modified': sample_date_filter_query['time_created']},
                ]
            }

        print(f'\nSingle-cell statistics{suffix}:\n')

        for checklist_type in schema_types:
            print(f'\n   Schema type: {checklist_type}\n')

            # ______________________________________

            record_count = self.single_cell_collection.count_documents(
                {'schema_name': checklist_type, **single_cell_date_filter_query}
            )
            print(f'      Number of submissions: {record_count}\n')

            # ______________________________________

            affiliations = self.single_cell_collection.distinct(
                'components.person.affiliation',
                {'schema_name': checklist_type, **single_cell_date_filter_query},
            )
            print(
                f'      Distinct affiliations: {len(affiliations)}\n'
                f'\t(like {join_with_and(list(affiliations)[:self.max_items_to_display])})\n'
            )

            # ______________________________________

            checklist_options = self.single_cell_collection.distinct(
                'checklist_id',
                {'schema_name': checklist_type, **single_cell_date_filter_query},
            )
            print(
                f'      Distinct checklist types: {len(checklist_options)}\n'
                f'\t(like {join_with_and(list(checklist_options)[:self.max_items_to_display])})\n'
            )

            # ______________________________________

            if checklist_type == 'COPO_SINGLE_CELL':
                technologies = self.single_cell_collection.distinct(
                    'components.study.technology',
                    {'schema_name': checklist_type, **single_cell_date_filter_query},
                )
                print(
                    f'      {len(technologies)} technologies are used (like {join_with_and(technologies[:self.max_items_to_display])})\n'
                )

            # ______________________________________

            for repository in self.submission_repositories:
                for status in self.single_cell_status:
                    result = self.single_cell_collection.distinct(
                        f'components.study.accession_{repository}',
                        {
                            'schema_name': checklist_type,
                            f'components.study.state_{repository}': status,
                            **single_cell_date_filter_query,
                        },
                    )
                    print(
                        f'      {len(result)} {status} submissions in {repository.upper()} ({join_with_and(result)})'
                    )

                print('\n      .......\n')

            print('\n________________________________________\n')

        print('\n________________________________________\n')


class SourceStatistics(MongoDB):
    # Count the number of sources/specimens records
    def get_specimen_statistics(self):
        print('Total number of specimens')
        count = self.source_collection.count_documents(
            {'sample_type': {'$nin': self.tol_specimen_types}}
        )
        label = join_with_and([item.upper() for item in self.non_tol_sample_types_list])
        print(f'   {label} specimens: {count}')
        for x in self.tol_sample_types:
            count = self.source_collection.count_documents(
                {'sample_type': x + '_specimen'}
            )
            print(f'   {x.upper()} specimens: {count}')
        print('\n________________________________________\n')

    # ______________________________________


class UserStatistics(MongoDB):
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
            'User ID': '_id',
            'First name': 'first_name',
            'Last name': 'last_name',
            'Email address': 'email',
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

        print(f"\nTop {max_users} users ranked by {primary_field.replace('_', ' ')}:\n")

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
    def get_email_addresses_of_registered_users(self, only_with_profiles=True):
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
        user_email_map = {user['id']: user['email'] for user in users}
        # Define table headers and data
        table_data = []
        table_headers = ['User ID', 'Email address']

        # Identify user IDs with no email address
        users_with_no_email = [
            user_id for user_id, email in user_email_map.items() if not email
        ]

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
            print(
                f'\nWarning: {len(users_with_no_email)} user IDs have no email address {msg_suffix}:\n'
            )
            print(
                tabulate(
                    [[str(user_id)] for user_id in users_with_no_email],
                    headers=['User ID'],
                    tablefmt='grid',
                )
            )

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

    # ______________________________________

    # Get count of samples submitted per user
    def get_average_samples_submitted_per_user(self, apply_date_filter=False):
        # Build date filter and get the date period for display
        date_filter_query, date_period = self.build_date_filter(
            apply_date_filter=apply_date_filter
        )
        suffix = f' {date_period}' if apply_date_filter else ''

        pipeline = [
            # Only get samples that have been accepted i.e. been submitted already
            {
                '$match': {
                    'status': 'accepted',
                    **date_filter_query,
                }
            },
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

        print('\nAverage number of samples submitted per user:\n')
        if result:
            average_samples = result[0]['average_samples_per_user']
            print(
                f'   {average_samples:.2f} average per user across {result[0]["total_users"]} users{suffix}'
            )
        else:
            print(f'   No sample data found to calculate average')

        print('\n________________________________________\n')

    # ______________________________________


# The class must be named Command and subclass BaseCommand
class Command(BaseCommand):
    # Show this when the user types help
    help = 'Get statistics of records in COPO'

    def handle(self, *args, **options):
        self.stdout.write('\nRunning statistics...')
        self.stdout.write('\n________________________________________\n')

        # ______________________

        # Initialise the classes to access the statistics functions
        profile_stats = ProfileStatistics()
        sample_stats = SampleStatistics()
        single_cell_stats = SingleCellStatistics()
        source_stats = SourceStatistics()
        user_stats = UserStatistics()

        ## Date-related statistics
        # sample_stats.get_distinct_scientific_names(apply_date_filter=True)
        # sample_stats.get_distinct_scientific_names_grouped_by_gal(
        #     apply_date_filter=True
        # )
        sample_stats.get_sample_statistics_between_dates()
        # sample_stats.get_sample_statistics_by_associated_project(apply_date_filter=True)
        # user_stats.get_average_samples_submitted_per_user(apply_date_filter=True)
        # sample_stats.get_sequencing_centres_used_for_sample_submission(
        #     apply_date_filter=True
        # )
        # sample_stats.get_gal_partner_used_for_sample_submission(apply_date_filter=True)
        # single_cell_stats.get_single_cell_statistics(
        #     schema_type=None, apply_date_filter=True
        # )

        # ______________________

        ## ERGA sample statistics
        # sample_stats.get_sample_statistics(sample_type='erga')
        # sample_stats.get_sample_statistics_between_dates(sample_type='erga')
        # sample_stats.get_sample_statistics_by_associated_project(sample_type='erga')

        # ______________________

        ## Profile statistics
        profile_stats.get_profile_statistics()
        # profile_stats.rank_genomic_profiles_and_get_owner_email()

        # ______________________

        # Sample statistics
        sample_stats.get_sample_statistics()
        sample_stats.get_sample_statistics_by_associated_project()
        sample_stats.get_distinct_scientific_names()
        # sample_stats.get_distinct_scientific_names_grouped_by_gal()

        # ______________________

        ## Single-cell statistics
        single_cell_stats.get_single_cell_statistics(
            schema_type=None, apply_date_filter=False
        )
        # single_cell_stats.get_single_cell_statistics(
        #     schema_type='COPO_SINGLE_CELL', apply_date_filter=False
        # )

        # ______________________

        ## Source statistics
        source_stats.get_specimen_statistics()

        # ______________________

        ## User statistics
        user_stats.get_average_samples_submitted_per_user()
        # user_stats.rank_users_by_samples_and_data_files_submitted(
        #     start_from='samples', max_users=10
        # )
        # user_stats.rank_users_by_samples_and_data_files_submitted(
        #     start_from='data_files', max_users=10
        # )

        ## Get only the email address of users linked to profiles
        # user_stats.get_email_addresses_of_registered_users()

        ## Get email address of all users
        # user_stats.get_email_addresses_of_registered_users(only_with_profiles=False)

    # ______________________________________
