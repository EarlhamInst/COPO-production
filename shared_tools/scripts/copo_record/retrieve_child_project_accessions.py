import logging
import os
import pymongo
import requests
import urllib.parse
import xml.etree.ElementTree as ET


def main():
    # Set up logging configuration
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Set MongoDB connection
    username = urllib.parse.quote_plus('copo_user')
    password = urllib.parse.quote_plus('password')
    myclient = pymongo.MongoClient(
        'mongodb://%s:%s@copo_mongo:27017/' % (username, password)
    )
    mydb = myclient['copo_mongo']

    # Define DB collections
    profile_collection = mydb['Profiles']
    sample_collection = mydb['SampleCollection']
    submission_collection = mydb['SubmissionCollection']

    # Parent or umbrella project accessions on ENA
    umbrella_project_accessions = {
        'erga': 'PRJEB43510',
        'dtol': 'PRJEB41134',
        'ERGA-PILOT': 'PRJEB47820',
    }

    # Define the project type and associated project
    sample_type = 'erga'
    associated_project = 'ERGA-PILOT'

    # Functions
    def fetch_profile_ids_for_project():
        profile_id_list = []

        # Get the profile IDs from the SampleCollection
        samples_cursor = sample_collection.distinct(
            'profile_id',
            {
                'sample_type': sample_type,
                'PRIMARY_BIOGENOME_PROJECT': {
                    '$regex': associated_project,
                    '$options': 'i',
                },
            },
        )

        # Get the profile IDs from the Profiles collection
        profile_cursor = profile_collection.find(
            {
                'associated_type': {'$regex': 'ERGA_PILOT', '$options': 'i'},
                'type': sample_type,
            },
            {'_id': 1},
        )
        profile_id_list = [str(profile['_id']) for profile in profile_cursor]

        # Merge the two lists
        profile_id_list = list(set(profile_id_list + samples_cursor))

        return profile_id_list

    def fetch_public_project_accessions_from_submissions(profile_id_list):
        # Get the accessions from the SubmissionCollection
        submission_cursor = submission_collection.find(
            {
                'profile_id': {'$in': profile_id_list},
                'accessions.project.status': 'PUBLIC',
            },
            {'accessions.project.accession': 1, '_id': 0},
        )

        project_accessions = []
        for doc in submission_cursor:
            projects = doc.get('accessions', {}).get('project', [])
            for project in projects:
                if 'accession' in project:
                    project_accessions.append(project['accession'])
        return project_accessions

    # Check the child projects have already been added to the umbrella project
    def get_missing_child_project_accessions(project_accessions):
        response = requests.get(
            f'https://www.ebi.ac.uk/ena/browser/api/xml/{umbrella_project_accessions[associated_project]}?includeLinks=false',
            headers={'accept': 'application/xml'},
        )

        # Optionally, print or handle errors
        if response.status_code != 200:
            logger.error('Error retrieving XML response: %s', response.status_code)
        else:
            try:
                # Parse the XML
                root = ET.fromstring(response.text)

                # Extract CHILD_PROJECT accessions from XML
                existing_child_accessions = {
                    child.attrib['accession']
                    for child in root.findall('.//CHILD_PROJECT')
                }

                # Determine which accessions have not been added
                missing_accessions = [
                    accession
                    for accession in project_accessions
                    if accession not in existing_child_accessions
                ]
                return missing_accessions
            except ET.ParseError as e:
                logger.error('Error parsing XML response: %s', e)
                return []

    profile_id_list = fetch_profile_ids_for_project()
    project_accessions = fetch_public_project_accessions_from_submissions(
        profile_id_list
    )
    child_project_accessions_to_be_added = get_missing_child_project_accessions(
        project_accessions
    )

    if child_project_accessions_to_be_added:
        # Output to a file
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, 'child_project_accessions_to_be_added.txt')
        with open(output_path, 'w') as f:
            for accession in child_project_accessions_to_be_added:
                f.write(f'{accession}\n')
        logger.info(
            "Child project accessions to be added stored in the path, '%s'",
            output_path,
        )
        return child_project_accessions_to_be_added
    else:
        logger.info('No child project accessions to be added.')


if __name__ == '__main__':
    main()
