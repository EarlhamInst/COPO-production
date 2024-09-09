from src.apps.copo_core.models import SequencingCentre
from common.dal.submission_da import Submission
from src.apps.copo_read_submission.utils.ena_read_submission import EnaReads
from common.dal.sample_da import Sample

def pre_save_erga_profile(auto_fields):
    associated_profiles = auto_fields.get("copo.profile.associated_type", [])
    sequence_centres = auto_fields.get("copo.profile.sequencing_centre", [])
    sequence_centre_need_approval = SequencingCentre.objects.filter(is_approval_required=True, name__in=sequence_centres)
    if sequence_centre_need_approval:
        for sc in sequence_centre_need_approval:
            if sc.name not in associated_profiles:
                associated_profiles.append(sc.name)
        auto_fields["copo.profile.associated_type"] = associated_profiles
        return {"status": "success"} 


def post_save_dtol_profile(profile):
    if profile["date_created"] != profile["date_modified"]:
        associated_type_lst = profile.get("associated_type", [])

        # Get associated type(s) as string separated by '|' symbol
        associated_type = " | ".join(associated_type_lst)

        # Update the 'associated_tol_project' field for unaccepted sample record(s) (if any exist)
        #is_associated_tol_project_update_required =  Sample().is_associated_tol_project_update_required(profile_id=kwargs["target_id"], new_associated_tol_project=associated_type)
        
        #if is_associated_tol_project_update_required:
        Sample().update_associated_tol_project(profile_id=str(profile["_id"]), associated_tol_project=associated_type)

        #update ENA project 
        submissions = Submission().get_all_records_columns(filter_by={"profile_id": str(profile["_id"])}, projection={"accessions":1})
        if submissions:
            project_accession = submissions[0].get("accessions",[]).get("project",[])
            if project_accession:
                result = EnaReads(submission_id=str(submissions[0]["_id"])).register_project()
                if result.get("status", False):
                    return {"status": True} 
                else:
                    return  {"status":"warning", "message":"Profile has been saved. However, profile ENA submission failed! " + result.get("message", str())}
        
    return {"status": "success"}
 