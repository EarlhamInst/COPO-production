'''
Samples & Sources Updater Script
==========================================

This script parses structured text-based update requests and generates MongoDB
update queries for two collections: `SampleCollection` and `SourceCollection`.

It supports:
    - Mapping biospecimen (sources) and biosample (samples) identifiers
    - Converting species-related metadata (e.g. taxonid (TAXON_ID), tolid (public_name), species name (SCIENTIFIC_NAME))
    - Optional execution of updates in MongoDB
    - Basic error handling for malformed or ambiguous input

Field Mappings:
---------------
See the MAPPINGS dictionary in the code for how input fields are mapped to MongoDB field names.

Usage:
------
1. Set your MongoDB connection string and database name in the `helpers.py` file.
2. Set `APPLY_TO_DB = True` in the `helpers.py` file, if you would like to apply
   updates, or leave as False for dry-run.
3. Provide structured text input for each update request block in the input file, `data.txt`.
   It is located in the same directory as this script.

   See the `Structure of update requests` subsection in the
   `Update Request Input` below for details.
4. Run the script to preview or apply MongoDB updates via the command line:
   ```bash
   cd shared_tools/scripts/copo_record/update_requests
   python handle_update_requests.py
   ```
5. Optionally provide additional taxonomy fields like GENUS, ORDER_OR_GROUP, and FAMILY
    like in the example below.

Outputs:
--------
- Updates are written to a file named `update_request_<date>.txt` in the same directory.
- Errors are logged to `update_request_errors_<date>.log`.
- COPO MongoDB update queries are generated and can be applied to the database.
- COPO find MongoDB queries are generated to retrieve the updated records and
  verify that the updates were successful.
- ENA update data is generated in the same file to complement the COPO updates.
  * The ENA update data i.e. the 'data' list,  is used by
    the `shared_tools/scripts/ena_record/updateEnaSubmission.py` script

Example update request:
-----------------------
```
    biospecimen
    SAMEA12345

    biosamples
    SAMEA67890
    SAMEA67891
    SAMEA67892
    SAMEA67893

    Update
    Species name: Example Scientific name
    taxonid: 999999
    tolid: publicNameEx
    GENUS: GenusName
    ORDER_OR_GROUP: OrderOrGroupName
    FAMILY: FamilyName
    ---
    biospecimen
    SAMEA67890

    biosamples
    SAMEA67894

    Update
    Species name: Example Scientific name
    taxonid: 999999
    tolid: publicNameEx
    GENUS: GenusName
    ORDER_OR_GROUP: OrderOrGroupName
    FAMILY: FamilyName
```

Notes:
------
    - The script expects consistent formatting: either tab-separated or colon-separated key-value pairs.
    - Request blocks without clear sample/source separation will raise an error.
    - You may optionally update other taxonomy fields like GENUS, ORDER_OR_GROUP and FAMILY.
    - Only fields - TAXON_ID and public_name are updates for SourceCollection.
    - Only fields - SCIENTIFIC_NAME, TAXON_ID, and public_name are updates for SampleCollection
      as well as optional fields - GENUS, ORDER_OR_GROUP and FAMILY.
'''

import json
import os
import sys

from helpers import *


# === Functions ===
'''
Read update requests from a file named 'data.txt'
in the same directory as this script.
The file should contain structured update requests separated by '---' where
each request is a string representing the structure defined above.
'''


def load_update_requests():
    if not os.path.exists(INPUT_FILE_NAME):
        message = f'Input file {INPUT_FILE_NAME} does not exist. Please create it with update requests.'
        print(message)
        log.error(message)
        sys.exit(1)

    with open(INPUT_FILE_NAME, 'r') as f:
        content = f.read()

    requests = [
        block.strip() for block in content.strip().split('---') if block.strip()
    ]
    return requests


def process_update_request(entry):
    samples, sources = get_samples_and_sources(entry)
    metadata, species_list = parse_update_block(entry)

    fields = {**metadata, **species_list}  # Merge

    updates = []

    for sample in samples:
        update = generate_sample_query(sample, fields)
        if update:
            updates.append(update)
        else:
            continue  # Skip if no valid update

    for source in sources:
        update = generate_source_query(source, fields)
        if update:
            updates.append(update)
        else:
            continue  # Skip if no valid update

    # Check for valid samples and sources
    if not samples and not sources:
        log.error(f'No sample(s) and source found in entry: {entry}')
        return

    if not samples:
        log.info(f'No sample(s) found in entry: {entry}')

    if not sources:
        log.info(f'No source found in entry: {entry}')

    # Output or apply
    if all(not d for d in updates):
        log.error(f'No valid updates generated for entry: {entry}')
        return

    # Filter out empty updates
    non_empty_updates = [d for d in updates if d]

    # Check for existing updates to avoid writing duplicate
    # entries to the file or applying them to the database
    should_abort, non_empty_updates = contains_existing_updates(non_empty_updates)

    if should_abort:
        return  # All entries are duplicates i.e. both incoming and existing entries in the file

    for update in non_empty_updates:
        # Apply the update to MongoDB if APPLY_TO_DB is True
        if APPLY_TO_DB:
            mongo_collection = (
                source_collection
                if update['collection'] == 'SourceCollection'
                else sample_collection
            )

            result = mongo_collection.update_one(update['query'], update['update'])

            print(
                f"\nDB update: biosampleAccession: {update['query']['biosampleAccession']}, Collection: {update['collection']}, Matched: {result.matched_count}, Modified: {result.modified_count}"
            )

        # Write the update to a text file
        if WRITE_TO_FILE:
            with open(OUTPUT_FILE_NAME, 'a') as f:
                f.write(
                    f"db.{update['collection']}.updateOne({update['query']}, {update['update']})"
                )
                f.write('\n')

            # log.info(
            #     f"Update written to {OUTPUT_FILE_NAME} for {update['collection']}. Please check the file for details."
            # )

            # Write the corresponding findOne query to the same file
            write_find_one_query(update)

            # Generate ENA 'data.json' to compement the 'shared_tools/scripts/ena_record/updateEnaSubmission.py' script
            # to update the records in ENA
            generate_ena_update_data_json()


def rearrange_update_request_file():
    '''
    Processes and filters update requests before writing them to the output file,
    ensuring that duplicates (based on previously written ENA update data) are removed.
    '''

    # Declare variables
    update_queries = []
    find_one_queries = []
    combined_data = []

    current_json_lines = []
    inside_json_block = False
    collecting_find_one_queries = False

    # Read the entire content first
    with open(OUTPUT_FILE_NAME, 'r') as file:
        lines = file.readlines()

    # Process the content
    for line in lines:
        stripped = line.strip()

        if '# COPO Find Data' in stripped:
            collecting_find_one_queries = True
            continue

        if '# ENA Update Data' in stripped:
            inside_json_block = True
            current_json_lines = []
            continue

        # Catch new sections to stop JSON collection
        if inside_json_block and (
            stripped.startswith('#') or stripped.startswith('----') or stripped == ''
        ):
            inside_json_block = False
            json_str = ''.join(str(line) for line in current_json_lines).strip()
            json_match = re.search(r'{\s*"data"\s*:\s*\[.*?\]\s*}', json_str, re.DOTALL)

            if json_match:
                # Try to parse JSON whenever it looks like it ends
                try:

                    json_data = json.loads(json_match.group())
                    combined_data.extend(json_data.get('data', []))
                except json.JSONDecodeError:
                    log.warning(
                        "Failed to parse an ENA data block during rearrangement:\n%s",
                        json_str,
                    )
            current_json_lines = []

        # Accumulate ENA JSON lines
        if inside_json_block:
            current_json_lines.append(line)

        # COPO queries block (SampleCollection, SourceCollection)
        if stripped.startswith('db.SampleCollection') or stripped.startswith(
            'db.SourceCollection'
        ):
            # COPO updateOne and fineOne queries block
            if 'updateOne' in stripped:
                update_queries.append(stripped)
            elif 'findOne' in stripped and collecting_find_one_queries:
                find_one_queries.append(stripped)

    # Catch last JSON block if file ends without a new section
    if inside_json_block and current_json_lines:
        try:
            json_str = ''.join(current_json_lines).strip()
            json_data = json.loads(json_str)
            combined_data.extend(json_data.get('data', []))
        except json.JSONDecodeError:
            log.warning('Failed to parse the final ENA data block.')

    # Deduplicate ENA data section before writing
    seen = set()
    deduped_data = []
    for record in combined_data:
        key = json.dumps(record, sort_keys=True)
        if key not in seen:
            seen.add(key)
            deduped_data.append(record)

    if len(deduped_data) < len(combined_data):
        removed = len(combined_data) - len(deduped_data)
        message = f'Removed {removed} duplicate record(s) from ENA update data.'
        log.warning(message)
        print(message)

    # Compose the new content
    output_lines = []
    output_lines.append(f'---- COPO Update Data ({len(update_queries)} records) ----')
    output_lines.extend(update_queries)
    output_lines.append('\n' + '_' * 40 + '\n')
    output_lines.append(f'---- COPO Find Data ({len(find_one_queries)} records) ----')
    output_lines.extend(find_one_queries)
    output_lines.append('\n' + '_' * 40 + '\n')
    output_lines.append(f'---- ENA Update Data ({len(deduped_data)} records) ----')
    output_lines.append(json.dumps({'data': deduped_data}, indent=4))

    # Write back to the same file (overwrite)
    with open(OUTPUT_FILE_NAME, 'w') as f:
        f.write('\n'.join(output_lines))


# === Update Request Input ===
'''
Structure of update requests:
    - Each request block starts with biospecimen or biosamples identifiers.
    - Followed by an 'Update' section with key-value pairs for metadata.
    - Each key-value pair is either tab-separated or colon-separated.
    - The script will parse these requests and generate MongoDB update queries.
    - If a request block is empty, it will be skipped.
    - If a request block does not contain valid samples or sources, it will raise an error
    - Optionally provide additional taxonomy fields like GENUS, ORDER_OR_GROUP, and FAMILY 
      like in the example below.
      
      Please note that in order to know what the corrrect optional taxonomy fields are, 
      1) Retrieve the existing metadata for any of the sample records in the update request 
         via the API URL: https://copo-project.org/api/sample/biosampleAccession/<biosample_accession>
         Replace `<biosample_accession>` with the actual biosample accession of the sample
      2) Take note of the existing taxonomy fields and as well as the fields in update 
         request and record them in a sample manifest.
      3) Upload the sample manifest to any profile matching the same type of the manifest to COPO 
         so that the validation can be done. The validation will determine if the provided metadata is valid. 
         If any of the optional taxonomy values are invalid, record them in the update request like the example below
      
    Structured example:
        biospecimen
        SAMEAxxxxxxx

        biosamples
        SAMEAyyyyyyy
        SAMEAzzzzzzz

        Update
        Species name: SCIENTIFIC_NAME X
        taxonid: 12345
        tolid: public_name1
        GENUS: GenusName
        ORDER_OR_GROUP: OrderOrGroupName
        FAMILY: FamilyName
'''


# === Run ===
def main():
    # Check and handle existing update request log files
    remove_existing_update_request_logs()

    # Create the error log file again
    # It accidentally got deleted in the remove_existing_update_request_logs function
    # and was not recreated
    if not os.path.exists(LOG_FILE_NAME):
        setup_logger(LOG_FILE_NAME)

    # Load update requests from the file
    update_requests = load_update_requests()

    # Process each update request
    print(f"Processing Update Request:\n{'-' * 30}")
    total_requests_count = len(update_requests)
    for i, request in enumerate(update_requests, start=1):
        # Check if the update request is empty
        if not request.strip():
            print(f'Request {i + 1} is empty. Skipping.')
            continue

        # === PROCESSING REQUESTS ===
        # Given that the enumeration starts at 1, i can be used directly
        # to display the request number
        print(f'\n#{i}/{total_requests_count}')
        process_update_request(request)

    # Rearrange the update request file
    rearrange_update_request_file()

    print(f'\n\nUpdate requests processed.')
    print(f'\t- Check for details in {OUTPUT_FILE_NAME}')
    print(f'\t- Check for errors in {LOG_FILE_NAME}')


if __name__ == '__main__':
    main()
