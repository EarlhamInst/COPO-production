import glob
import json
import logging
import re
import pymongo
import os
import sys
import urllib.parse

from datetime import date, datetime

# === File and directory configuration ===
# Get today's date in in the format: YYYY-MM-DD
TODAY_STR = date.today().isoformat()
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Create the filename with the date
OUTPUT_FILE_NAME = os.path.join(SCRIPT_DIR, f'update_request_{TODAY_STR}.txt')
LOG_FILE_NAME = os.path.join(SCRIPT_DIR, f'update_request_errors_{TODAY_STR}.log')
INPUT_FILE_NAME = os.path.join(SCRIPT_DIR, 'data.txt')

# === Logging configuration ===
logging.basicConfig(
    filename=LOG_FILE_NAME,
    filemode='a',  # Append mode
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
)

log = logging.getLogger(__name__)

# === General configuration ===
APPLY_TO_DB = False  # Set to True to apply the updates
WRITE_TO_FILE = True  # Set to True to write updates to a file

# === MongoDB Connection ===
username = urllib.parse.quote_plus('copo_user')
password = urllib.parse.quote_plus('password')
myclient = pymongo.MongoClient(
    'mongodb://%s:%s@copo_mongo:27017/' % (username, password)
)
mydb = myclient['copo_mongo']
sample_collection = mydb['SampleCollection']
source_collection = mydb['SourceCollection']

# === Field Mappings ===
SAMPLE_FIELD_NAMES = ['biosample', 'sample', 'biosamples']
SOURCE_FIELD_NAMES = ['biospecimen', 'specimen']
MAPPINGS = {
    'biosample_accession': 'biosampleAccession',
    'biosample': 'biosampleAccession',
    'biospecimenac': 'biosampleAccession',
    'biospecimen': 'biosampleAccession',
    'Species name': 'SCIENTIFIC_NAME',
    'Species': 'SCIENTIFIC_NAME',
    'New Species Name': 'SCIENTIFIC_NAME',
    'taxonid': 'TAXON_ID',
    'taxon': 'TAXON_ID',
    'New TaxonID': 'TAXON_ID',
    'tolid': 'public_name',
    'New TOLID': 'public_name',
}

ENA_MAPPINGS = {
    'sraAccession': 'sample_accession',
    'public_name': 'tolid',
}

OPTIONAL_TAXONOMY_UPDATE = ['GENUS', 'ORDER_OR_GROUP', 'FAMILY']
TAXONOMY_FIELDS = ['SCIENTIFIC_NAME', 'GENUS', 'ORDER_OR_GROUP', 'TAXON_ID', 'FAMILY']


# === Classes ===
class MicrosecondFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            # Use datetime formatting with microseconds
            s = datetime.fromtimestamp(record.created).strftime(datefmt)
        else:
            s = super().formatTime(record, datefmt)
        return s


# === Functions ===
def contains_existing_updates(new_updates: list) -> tuple[bool, list]:
    '''
    Checks if any of the new updates already exist in the OUTPUT_FILE_NAME.
    - If all updates are duplicates, abort the process
    - If some updates are duplicates, keep only non-duplicates and proceed with those

    Returns:
        (True, [])    -> if all updates are duplicates
        (False, list) -> with only non-duplicate updates
    '''

    if not os.path.exists(OUTPUT_FILE_NAME):
        return False, new_updates  # No file == no duplicates

    with open(OUTPUT_FILE_NAME, 'r') as f:
        existing_content = f.read()

    update_strs = [
        f"db.{update['collection']}.updateOne({update['query']}, {update['update']})"
        for update in new_updates
    ]

    duplicates = [s for s in update_strs if s in existing_content]

    # === Case 1: All updates exist already ===
    if len(duplicates) == len(update_strs):
        log.warning(
            'All updates already exist in the output file. No new updates recorded.'
        )
        print('No new updates — all entries are duplicates. Exiting.')
        return True, []

    # === Case 2: Some updates already exist not all ===
    elif duplicates:
        log.warning(
            f'{len(duplicates)} duplicate update(s) found. These will be skipped.'
        )
        print(f'{len(duplicates)} duplicate update(s) skipped.')

        # Retain only non-duplicates
        non_duplicates = [
            update for update, s in zip(new_updates, update_strs) if s not in duplicates
        ]
        return False, non_duplicates

    return False, new_updates


def extract_existing_ena_data():
    '''
    Extract all existing ENA records from the OUTPUT file for comparison.
    Assumes they are written in JSON chunks like:
        # ENA Update Data
        { 'data': [...] }
    '''
    existing_data = []

    with open(OUTPUT_FILE_NAME, 'r') as f:
        content = f.read()

    # Use regex to extract each ENA data block
    matches = re.findall(r'# ENA Update Data\s*\n({.*?})', content, re.DOTALL)
    for match in matches:
        try:
            block = json.loads(match)
            existing_data.extend(block.get('data', []))
        except json.JSONDecodeError:
            continue

    return existing_data


def generate_ena_update_data_json():
    # Generates a JSON file for ENA updates based on the update request file
    with open(OUTPUT_FILE_NAME) as f:
        lines = f.readlines()
    data = []

    for line in lines:
        stripped = line.strip()  # Remove any leading/trailing whitespace

        # Extract and convert updateOne (or update_one) query to find_one
        record = parse_update_query_into_find_query(stripped)
        if record:
            data.append(record)

    # Load existing ENA data from previous runs
    existing_data = extract_existing_ena_data()

    # Build a set of seen records from existing data
    seen = {json.dumps(r, sort_keys=True) for r in existing_data}

    # Remove duplicates based on what's already in the file
    new_deduped = []
    for record in data:
        key = json.dumps(record, sort_keys=True)
        if key not in seen:
            seen.add(key)
            new_deduped.append(record)

    if not new_deduped:
        log.warning('No new ENA updates found — all are duplicates of existing data.')
        return

    result = {'data': new_deduped}

    # Append the result to the existing file
    with open(OUTPUT_FILE_NAME, 'a') as f:
        f.write(f"\n\n{'-' * 30}\n")
        f.write('# ENA Update Data\n')
        json.dump(result, f, indent=4)
        f.write('\n')

    log.info(f'Added {len(new_deduped)} new ENA update(s) to {OUTPUT_FILE_NAME}.')


def generate_sample_query(sample, fields):
    query = {'biosampleAccession': sample}
    update = {'$set': {}}

    # Handle provided taxonomy fields
    if fields.get('TAXON_ID', ''):
        update['$set']['TAXON_ID'] = fields.get('TAXON_ID')
        update['$set']['species_list.0.TAXON_ID'] = fields.get('TAXON_ID')

    if fields.get('SCIENTIFIC_NAME', ''):
        update['$set']['SCIENTIFIC_NAME'] = fields.get('SCIENTIFIC_NAME')
        update['$set']['species_list.0.SCIENTIFIC_NAME'] = fields.get('SCIENTIFIC_NAME')

    if fields.get('public_name', ''):
        update['$set']['public_name'] = fields.get('public_name')

    # Handle optional taxonomy updates
    for x in OPTIONAL_TAXONOMY_UPDATE:
        if fields.get(x, ''):
            value = fields.get(x)
            if value:
                update['$set'][x] = value
                update['$set'][f'species_list.0.{x}'] = value
            else:
                log.info(
                    f'[WARNING] {x} is empty for sample {sample}. Skipping update.'
                )
                continue

    if query['biosampleAccession'] and update['$set']:
        # Return the query and update structure for SampleCollection
        return {'collection': 'SampleCollection', 'query': query, 'update': update}
    else:
        # If no valid update, return an empty dict
        log.error(
            f'No valid update for sample {sample}. Query: {query}, Update: {update}'
        )
        return {}


def generate_source_query(source, fields):
    query = {'biosampleAccession': source}
    update = {'$set': {}}

    if fields.get('TAXON_ID', ''):
        update['$set']['TAXON_ID'] = fields.get('TAXON_ID')

    if fields.get('public_name', ''):
        update['$set']['public_name'] = fields.get('public_name')

    if query['biosampleAccession'] and update['$set']:
        return {'collection': 'SourceCollection', 'query': query, 'update': update}
    else:
        # If no valid update, return an empty dict
        log.error(
            f'No valid update for source {source}. Query: {query}, Update: {update}'
        )
        return {}


def get_sample_record_by_source(source_accession, log, sample_collection):
    '''
    Retrieves a sample record from SampleCollection based on the given source/specimen biosample accession.
    This is used to get the scientific name since it is not present in the SourceCollection only in SampleCollection.
    Returns the scientific name if found, otherwise returns None.
    '''
    sample_record = sample_collection.find_one(
        {
            '$or': [
                {'sampleDerivedFrom': source_accession},
                {'sampleSameAs': source_accession},
                {'sampleSymbiontOf': source_accession},
            ]
        },
        {'_id': 0, 'SCIENTIFIC_NAME': 1},
    )

    if sample_record:
        return sample_record
    else:
        log.error(f'No sample found for the source with accession: {source_accession}')
        return None


def get_samples_and_sources(entry):
    '''
    Extracts sample and source accessions from structured update request.
    Parses everything before the 'Update' header.
    Returns biosamples and biospecimens as lists.

    Input format and structure:
        biospecimen
        SAMEAuuuuuuu

        biosamples
        SAMEAvvvvvvv
        SAMEAwwwwwww

        Update
        Species name: SCIENTIFIC NAME X
        taxonid: 12345
        tolid: publicName
        ---
        biospecimen
        SAMEAxxxxxxx

        biosamples
        SAMEAzzzzzzz

        Update
        Species name: SCIENTIFIC_NAME Y
        taxonid: 67890
        tolid: publicName
        GENUS: GenusName
        ORDER_OR_GROUP: OrderOrGroupName
        FAMILY: FamilyName
    '''
    if not isinstance(entry, str):
        raise TypeError("Expected 'entry' to be a string", entry)

    samples = []
    sources = []
    current_key = None

    for line in entry.strip().splitlines():
        line = line.strip()
        if not line:
            continue

        key_lower = line.lower()
        if key_lower == 'update':
            break

        if key_lower in SOURCE_FIELD_NAMES:
            current_key = 'biospecimen'
            continue
        elif key_lower in SAMPLE_FIELD_NAMES:
            current_key = 'biosamples'
            continue

        if line != current_key:
            if current_key == 'biospecimen':
                sources.append(line)

            if current_key == 'biosamples':
                samples.append(line)

    # Check for ambiguous entries
    if not samples and not sources:
        # Try to infer ambiguous entries
        all_ids = [
            v
            for k, v in entry.items()
            if re.match(r'^SAMEA\d+$', k) or re.match(r'^SAMEA\d+$', str(v))
        ]
        if all_ids:
            log.error(
                f'Ambiguous request. Cannot determine sample or source roles: {all_ids}'
            )
            return [], []
        return [], []
    return samples, sources


def parse_update_block(text):
    '''
    Parses key-value metadata below the 'Update' header in the update request entry,
    using MAPPINGS to translate keys to MongoDB field names.
    Also builds nested species_list.0.* fields, but only for taxonomy fields.
    '''
    assert isinstance(text, str), 'Input must be a string'

    metadata = {}
    species_list = {}

    in_update_section = False

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        if line.lower() == 'update':
            in_update_section = True
            continue

        if not in_update_section or ':' not in line:
            continue

        key, value = map(str.strip, line.split(':', 1))
        for raw_key, mapped_key in MAPPINGS.items():
            if key.lower() == raw_key.lower():
                metadata[mapped_key] = value
                if mapped_key in TAXONOMY_FIELDS:
                    species_list[f'species_list.0.{mapped_key}'] = value
                break

        # Handle optional taxonomy fields
        for optional_field in OPTIONAL_TAXONOMY_UPDATE:
            if key.lower() == optional_field.lower():
                metadata[optional_field] = value
                species_list[f'species_list.0.{optional_field}'] = value
                break
    return metadata, species_list


def parse_update_query_into_find_query(line):
    # Extract collection name, filter, and update dict from Mongo updateOne line
    match = re.match(
        r"db\.(\w+)\.updateOne\(\s*(\{.*?\})\s*,\s*\{\s*'\$set'\s*:\s*(\{.*\})\s*\}\s*\)",
        line,
        re.DOTALL,
    )
    if not match:
        return None

    collection, filter_str, update_str = match.groups()

    # Fix single quotes to double quotes for JSON parsing
    try:
        filter_json = json.loads(filter_str.replace("'", '"'))
        update_json = json.loads(update_str.replace("'", '"'))
    except json.JSONDecodeError as e:
        log.exception(f'Failed to parse JSON: {e}')
        return None

    if not collection:
        log.error(f'Could not parse: {line}')
        return None

    # Build projection
    # Always include 'sraAccession' and exclude '_id' field
    projection = {'_id': 0, 'sraAccession': 1}
    for key in update_json:
        # Skip unwanted keys
        excluded_fields = OPTIONAL_TAXONOMY_UPDATE + ['sraAccession']

        if key.startswith('species_list.0') or key in excluded_fields:
            continue
        projection[key] = 1

    # Execute find_one MongoDB query
    mongo_collection = (
        source_collection if collection == 'SourceCollection' else sample_collection
    )
    result = mongo_collection.find_one(filter_json, projection)

    if result:
        if collection == 'SourceCollection':
            sample_record = get_sample_record_by_source(
                filter_json['biosampleAccession'], log, sample_collection
            )
            if sample_record and 'SCIENTIFIC_NAME' in sample_record:
                # SCIENTIFIC_NAME is not present in SourceCollection, so it should be retrieved
                # from the SampleCollection to update it in ENA
                result['SCIENTIFIC_NAME'] = sample_record.get('SCIENTIFIC_NAME')

        # Map COPO field names to ENA field names
        for copo_field, ena_field in ENA_MAPPINGS.items():
            if copo_field in result:
                result[ena_field] = result.pop(copo_field)
        return result
    else:
        log.info(f'No document found in {collection} for filter: {filter_json}')


def remove_existing_update_request_logs():
    log_files = glob.glob('update_request_*.txt')
    error_files = glob.glob('update_request_errors*.log')
    all_files = log_files + error_files

    if all_files:
        # Automatically remove the single error log file if no other file exists
        if len(all_files) == 1 and error_files:
            try:
                os.remove(error_files[0])
            except Exception as e:
                print(f'Error deleting {error_files[0]}: {e}')

            return

        print(
            f'Found {len(log_files)} update request file(s) and {len(error_files)} update request error file(s):'
        )
        for f in all_files:
            print(f'  - {f}')

        attempts = 0
        while attempts < 3:
            choice = input('Do you want to delete these files? [y/n]: ').strip().lower()
            if choice in ['y', 'n']:
                break
            else:
                print("Invalid choice. Please enter 'y' or 'n'.")
                attempts += 1

        if attempts == 3:
            print('Too many invalid attempts. Exiting script.')
            sys.exit(1)

        if choice == 'y':
            for f in all_files:
                try:
                    os.remove(f)
                    print(f'Deleted: {f}')
                except Exception as e:
                    print(f'Error deleting {f}: {e}')
        else:
            print('Files not deleted.')


def setup_logger(log_file: str):
    log = logging.getLogger(__name__)
    log.setLevel(logging.INFO)

    # Remove existing handlers if any exists
    if log.hasHandlers():
        log.handlers.clear()

    # Create/Recreate the file handler after file deletion
    file_handler = logging.FileHandler(log_file, mode='a')

    # Custom formatter: 'LEVEL - [timestamp]: message'
    formatter = MicrosecondFormatter(
        fmt='%(levelname)s - [%(asctime)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S.%f'
    )
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)


def write_find_one_query(update):
    '''
    Writes a MongoDB findOne queries to the file using the projection
    keys from update['update']['$set'].
    '''
    # Extract the projection keys from the $set dict
    set_fields = update['update'].get('$set', {})

    # Modify the fields that have 'species_list.0' prefix so that the findOne query
    # can retrieve the correct fields. The correct projection field should be species_list.SCIENTIFIC_NAME for example
    set_fields = {
        (
            k.replace('species_list.0.', 'species_list.')
            if k.startswith('species_list.0.')
            else k
        ): v
        for k, v in set_fields.items()
    }

    projection = {'_id': 0}  # Exclude _id field by default
    projection = {key: 1 for key in set_fields.keys()}

    # Format the findOne query string
    find_one_str = f"db.{update['collection']}.findOne({update['query']}, {projection})"

    with open(OUTPUT_FILE_NAME, 'a') as f:
        f.write(f"\n\n{'-' * 30}\n")
        f.write('# COPO Find Data\n')
        f.write(find_one_str + '\n')

    log.info(f"Find query written to {OUTPUT_FILE_NAME} for {update['collection']}.")
