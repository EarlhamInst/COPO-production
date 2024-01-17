import common.schemas.utils.data_utils as d_utils
from common.dal.sample_da import Sample
from common.dal.submission_da import Submission

def generate_accessions_record(profile_id=str(), isUserProfileActive=str(), isSampleProfileTypeStandalone=str()):
    isUserProfileActive = d_utils.convertStringToBoolean(isUserProfileActive)
    isSampleProfileTypeStandalone = d_utils.convertStringToBoolean(isSampleProfileTypeStandalone)
    records = None
    data_set = list()
    columns = list()

    columns.append(dict(data="record_id", visible=False))
    columns.append(dict(data="DT_RowId", visible=False))
    columns.append(dict(data="accession_type", title="Accession Type", visible=True))

    if isSampleProfileTypeStandalone:
        if isUserProfileActive and profile_id:
            records = Submission().get_standalone_project_accessions(filter_by={"profile_id": profile_id})
        else:
            records = Submission().get_standalone_project_accessions()
        
        # Add profile_id column
        columns.append(dict(data="profile_id", visible=False))

        # Set column names
        labels = ["accession", "alias", "profile_title"]
        for x in labels:
            columns.append(dict(data=x, title=d_utils.convertStringToTitleCase(x)))

        # Declare labels for 'sample' accession
        sample_accession_labels = ['sample_accession','sample_alias']

        if records:
            # Records exist
            for i in records:
                for key, value in i.get('accessions','').items():
                    for accession in value:
                        row_data = dict()
                        row_data["record_id"] = i.get('id','')
                        row_data["DT_RowId"] = "row_" + i.get('id','')
                        row_data["profile_id"] = i.get('profile_id','')
                        row_data["accession_type"] = key
                        
                        # Filter value dictionary to get 'accession' and 'alias' key-value pair
                        if key == 'sample':
                            # Account for'sample' accession which has accession and alias in a different format 
                            value_dict = {k.split("_")[1]: v for k, v in accession.items() if k in sample_accession_labels}
                        else:
                            value_dict = {k: v for k, v in accession.items() if k in labels}

                        for k, v in value_dict.items():
                            row_data.update({k: v})
                        row_data.update({'profile_title': i.get('profile_title','')})
                        data_set.append(row_data)
                

            return_dict = dict(dataSet=data_set, columns=columns)
        else: 
            # No records found
            return_dict = dict(dataSet=data_set, columns=columns)
            
    else:
        if isUserProfileActive and profile_id:
            records = Sample().get_tol_project_accessions(filter_by={"profile_id": profile_id})
        else:
            records = Sample().get_tol_project_accessions()
        
        # Set column names
        labels = ['biosampleAccession', 'sraAccession', 'submissionAccession', 'manifest_id', 'SCIENTIFIC_NAME', 'SPECIMEN_ID', 'TAXON_ID']
        for x in labels:
            columns.append(dict(data=x, title=d_utils.convertStringToTitleCase(x)))

        if records:
            # Records exist
            for i in records:
                row_data = dict()
                row_data["record_id"] = i.get('_id','')
                row_data["DT_RowId"] = "row_" + i.get('_id','')
                row_data["accession_type"] = i.get('tol_project','')

                row_data.update({key: i.get(key,'') for key in i.keys() if key in labels})
                data_set.append(row_data)

            return_dict = dict(dataSet=data_set, columns=columns)
        else: 
            # No records found
            return_dict = dict(dataSet=data_set, columns=columns)

    return return_dict
