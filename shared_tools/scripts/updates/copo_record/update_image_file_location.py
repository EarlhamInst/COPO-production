import pymongo
import urllib.parse
import requests
import os

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

username = urllib.parse.quote_plus('copo_user')
password = urllib.parse.quote_plus('password')
myclient = pymongo.MongoClient(
    'mongodb://%s:%s@copo_mongo:27017/' % (username, password)
)

mydb = myclient['copo_mongo']
datafile_collection = mydb['DataFileCollection']
sample_collection = mydb['SampleCollection']

filter = {
    'bioimage_name': {'$exists': True, '$ne': ''},
    'bucket_name': {'$exists': False},
    'ecs_location': {'$exists': False},
    'file_location': {'$exists': True, '$ne': ''},
}
projection = {'file_location': 1, 'name': 1, 'bioimage_name': 1}
cursor = datafile_collection.find(filter, projection)


bia_image_file_prefix = (
    f'https://ftp.ebi.ac.uk/biostudies/fire/S-BIAD/012/S-BIAD1012/Files/'
)


@retry(
    stop=stop_after_attempt(5),  # Retry up to 5 times
    # Exponential backoff (2s, 4s, 8s etc.)
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.RequestException),
    reraise=True,  # Raise exception if all attempts fail
)
def check_and_save_bia_image_url(file_id, url):
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        if response.status_code == 200 and 'image' in response.headers.get(
            'Content-Type', ''
        ):
            return url  # URL is valid and leads to an image
    except requests.RequestException as e:
        print(f'Retrying due to error fetching image with ID, {file_id}: {e}')
        raise  # Raise exception to trigger retry
    return None  # URL does not exist or is not an image


for file in cursor:
    filter = {}
    file_location = file.get('file_location', '')
    filename = file.get('name', '')
    bioimage_name = file.get('bioimage_name', '')
    specimen = ''

    if bioimage_name:
        name_without_extension, _ = os.path.splitext(bioimage_name)
        specimen = name_without_extension.split('_')[0]
        filter = {
            '$or': [
                {'sampleDerivedFrom': specimen},
                {'sampleSameAs': specimen},
                {'sampleSymbiontOf': specimen},
            ]
        }
    else:
        name_without_extension, _ = os.path.splitext(filename)
        specimen_id = name_without_extension.split('_')[0]
        filter = {'SPECIMEN_ID': specimen_id}

    sample = sample_collection.find_one(
        filter,
        {
            'sample_type': 1,
            'sampleDerivedFrom': 1,
            'sampleSameAs': 1,
            'sampleSymbiontOf': 1,
            '_id': 0,
        },
    )
    sample_type = sample.get('sample_type', '') if sample else ''
    sample_type = 'DToL' if sample_type in ['asg', 'dtol'] else sample_type.upper()

    # Specimen is also known as the  source
    if not specimen:
        specimen = (
            sample.get(
                'sampleDerivedFrom',
                sample.get('sampleSameAs', sample.get('sampleSymbiontOf', '')),
            )
            if sample
            else ''
        )

    if sample_type and specimen:
        print('Checking for BIA image URL for:', file['_id'])
        bia_image_url = check_and_save_bia_image_url(
            file['_id'], f'{bia_image_file_prefix}{sample_type}/{bioimage_name}'
        )

        if (
            bia_image_url
            and not os.path.exists(file_location)
            and bia_image_url != file_location
        ):
            datafile_collection.update_one(
                {'_id': file['_id']}, {'$set': {f'file_location': bia_image_url}}
            )
            print(
                f"Updated image file (with document ID: { file['_id']}) location from '{file_location}' to  '{bia_image_url}'"
            )
