from collections import defaultdict
import os
import pandas as pd
import pymongo
import urllib.parse
import common.schemas.utils.data_utils as d_utils

from datetime import datetime
from django.core.management import BaseCommand
from pathlib import Path
from tabulate import tabulate

'''
To run this Django command, get_source_and_sample_records, execute the following in the terminal:
$ python manage.py get_source_and_sample_records


To copy file from VM to local machine, execute the following in the terminal:
$ ssh -i <ssh-key.pem> <vm-username>@<vm-name-or-ip-address> "sudo docker exec <container-id> cat <path-to-file-in-container-.ext>" > <path-for-file-on-local-machine-.ext>


To call an individual function from this Django command, get_source_and_sample_records,
execute the following in the terminal:
# Open Django interactive shell
$ python manage.py shell

# Import the Command class
from src.apps.copo_core.management.commands.get_source_and_sample_records import Command

# Allow breakpoints. Ensure that the function to be called has breakpoints set
import pdb; pdb.set_trace()

cmd = Command() # Instantiate the Command class
cmd.initialise_db() # Initialise the database

# Call any method inside the class
cmd.my_function(args_here)
# e.g. cmd.get_sample_records(sample_type='dtol')
# e.g. cmd.get_specimen_records(specimen_type='dtol')
________________________________________

To clear terminal in Python interactive shell, use the following command:
import os
os.system('cls' if os.name == 'nt' else 'clear')
'''


# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    # Show this when a user types help
    help = 'Get source/specimen and sample records from MongoDB and save them to Excel files.'

    def handle(self, *args, **options):
        self.stdout.write('\nRetrieving records...')
        self.stdout.write('\n________________________________________\n')

        # Setup MongoDB connection and load data
        self.initialise_db()

        # Retrieve records
        self.get_specimen_records(
            specimen_type='dtol',
        )
        self.get_specimen_records(
            specimen_type='asg',
        )
        self.get_sample_records(sample_type='dtol')
        self.get_sample_records(sample_type='asg')

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
            raise PermissionError(f'MongoDB authentication failed: {e}')
        except pymongo.errors.ServerSelectionTimeoutError as e:
            # Raised if server cannot be reached
            raise ConnectionError(f'Cannot connect to MongoDB: {e}')

        database = mongodb_client['copo_mongo']

        self.sample_collection = database['SampleCollection']
        self.source_collection = database['SourceCollection']

        # The keys match what ENA would like as column headers in the spreadsheet output
        # and the values match the keys in the MongoDB records
        self.field_map = {
            'SAMPLE_ID': 'sraAccession',
            'PUBLIC_NAME': 'public_name',
            'TAXON_ID': 'TAXON_ID',
            'FIRST_CREATED': 'date_created',
            'BIOSAMPLE_ID': 'biosampleAccession',
        }
        self.projection = {'_id': 0, **{x: 1 for x in self.field_map.values()}}
        self.table_headers = list(self.field_map.keys())

        self.field_map_samples = {**self.field_map, 'FIRST_CREATED': 'time_created'}
        self.projection_samples = {
            '_id': 0,
            'species_list.TAXON_ID': 1,
            **{x: 1 for x in self.field_map_samples.values()},
        }
        self.table_headers_samples = list(self.field_map_samples.keys())

        self.sample_types = self.sample_collection.distinct('sample_type')
        self.non_tol_sample_types = {'isasample': ['genomics', 'biodata']}
        self.non_tol_sample_types_list = list(self.non_tol_sample_types.values())[0]
        self.tol_sample_types = [
            x for x in self.sample_types if x not in self.non_tol_sample_types.keys()
        ]
        self.tol_specimen_types = [x + '_specimen' for x in self.tol_sample_types]

    # ______________________________________

    def create_excel_file_from_table_data(
        self, table_data, table_headers, file_name, prefix, sheet_name
    ):
        # Print the table using the 'tabulate' library
        # print(tabulate(table_data, headers=table_headers, tablefmt='grid'))

        # Create a DataFrame from the table data
        df = pd.DataFrame(table_data, columns=table_headers)

        # Check if the file exists and remove it if it does
        current_path = os.getcwd()
        # This can also be written as Path(current_path) / file_name
        file_path = os.path.join(current_path, file_name)
        directory = Path(current_path)

        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_file() and entry.name.startswith(prefix):
                    os.remove(entry.path)
                    print(f'\n   * Deleted: {entry.name}')

        # Write the DataFrame to an Excel file
        df.to_excel(file_path, index=False, sheet_name=sheet_name)
        print(
            f"\n   * Excel file '{file_path}' has been created in '{current_path}' directory."
        )

    # ______________________________________

    # Get sources/specimens records
    def get_specimen_records(self, specimen_type=None):
        '''
        Get ENA records i.e. records that have an accession

        Note: It's not wise for the query to include 'accepted' status because
        some records may have been rejected but still have an accession
        '''
        projection = self.projection | {'sample_type': 1}
        specimen_types = (
            self.tol_specimen_types
            if specimen_type is None
            else [f'{specimen_type.lower()}_specimen']
        )

        cursor = self.source_collection.find(
            {
                'sample_type': {'$in': specimen_types},
                # 'status': 'accepted',
                'biosampleAccession': {'$exists': True, '$ne': ''},
            },
            projection,
        )
        records = list(cursor)
        groups = defaultdict(list)

        if records:
            for record in records:
                sample_type = record.get('sample_type', '').replace('_specimen', '')

                # Ensure that the date_created field is formatted as a string in 'YYYY-MM-DD' format
                # Records with no field will have 'N/A' as the value for that field

                if isinstance(record.get('date_created'), datetime):
                    record['date_created'] = record['date_created'].strftime('%Y-%m-%d')

                # Reorder the fields in the record based on the field_map so
                # that the order of the headers match the order of the fields in the record
                new_record = {
                    table_header: (record.get(copo_field) if copo_field else 'N/A')
                    for table_header, copo_field in self.field_map.items()
                }
                groups[sample_type].append(new_record)

            for sample_type, group_records in groups.items():
                print(
                    f'\n{len(group_records)} {sample_type.upper()} specimen/source records were found'
                )

                # Sort records by date_created
                group_records.sort(
                    key=lambda r: r.get('date_created', datetime.min),
                )

                file_name_prefix = f'{sample_type}-biospecimen-records'
                file_name = (
                    f'{file_name_prefix}-{datetime.now().strftime('%Y-%m-%d')}.xlsx'
                )
                sheet_name = f'{sample_type.upper()} biospecimens'

                self.create_excel_file_from_table_data(
                    table_data=group_records,
                    table_headers=self.table_headers,
                    file_name=file_name,
                    prefix=file_name_prefix,
                    sheet_name=sheet_name,
                )
        else:

            print(
                f'No specimen records found for {d_utils.join_with_and(specimen_types)}'
            )

        print('\n________________________________________\n')

    # ______________________________________

    # Get sample records
    def get_sample_records(self, sample_type=None):
        '''
        Get ENA records i.e. records that have an accession

        Note: It's not wise for the query to include 'accepted' status because
        some records may have been rejected but still have an accession
        '''
        projection = self.projection_samples | {'sample_type': 1}
        sample_types = (
            self.sample_types if sample_type is None else [f'{sample_type.lower()}']
        )

        cursor = self.sample_collection.find(
            {
                'sample_type': {'$in': sample_types},
                # 'status': 'accepted',
                'biosampleAccession': {'$exists': True, '$ne': ''},
            },
            projection,
        )
        records = list(cursor)
        groups = defaultdict(list)

        if records:
            for record in records:
                record_sample_type = record.get('sample_type', '')

                # Handle cases where TAXON_ID is nested in 'species_list' list
                if all(k in record for k in ['species_list', 'TAXON_ID']):
                    # Remove the existing TAXON_ID if it exists
                    record.pop('species_list', None)

                if 'species_list' in record and 'TAXON_ID' not in record:
                    record['TAXON_ID'] = record['species_list'][0].get(
                        'TAXON_ID', 'N/A'
                    )
                    record.pop('species_list', None)

                if isinstance(record.get('time_created'), datetime):
                    record['time_created'] = record['time_created'].strftime('%Y-%m-%d')

                # Reorder the fields in the record based on the field_map_samples so
                # that the order of the headers match the order of the fields in the record

                # Ensure that the time_created field is formatted as a string in 'YYYY-MM-DD' format
                # Records with no field will have 'N/A' as the value for that field
                new_record = {
                    table_header: (record.get(copo_field) if copo_field else 'N/A')
                    for table_header, copo_field in self.field_map_samples.items()
                }
                groups[record_sample_type].append(new_record)

            for group_sample_type, group_records in groups.items():
                print(
                    f'\n{len(group_records)} {group_sample_type.upper()} sample records were found'
                )

                # Sort records by time_created
                group_records.sort(
                    key=lambda r: r.get('time_created', datetime.min),
                )

                file_name_prefix = f'{group_sample_type}-biosample-records'
                file_name = (
                    f'{file_name_prefix}-{datetime.now().strftime('%Y-%m-%d')}.xlsx'
                )
                sheet_name = f'{group_sample_type.upper()} biosamples'

                self.create_excel_file_from_table_data(
                    table_data=group_records,
                    table_headers=self.table_headers,
                    file_name=file_name,
                    prefix=file_name_prefix,
                    sheet_name=sheet_name,
                )
        else:
            print(f'No sample records found for {d_utils.join_with_and(sample_types)}')

        print('\n________________________________________\n')

    # ______________________________________
