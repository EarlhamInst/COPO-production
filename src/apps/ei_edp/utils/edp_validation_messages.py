MESSAGES = {
    'all_samples_submitted': (
        'All of the samples requested for this profile have already been submitted '
        'so, a blank manifest cannot be downloaded. If you would like to submit '
        'more samples, increase the number of samples on the profile.'
    ),
    'customer_not_authorised_to_join': (
        "Customer with email '{email}' is not authorised to join the profile."
    ),
    'invalid_sample_count': (
        "Number of samples must be a whole number of 0 or more, got '{sample_count}'."
    ),
    'missing_sapio_project': (
        'This profile has no Sapio project associated with it. '
        'Save the profile again to retry the project synchronisation to Sapio.'
    ),
    'missing_value_customer_email': (
        'Sheet <strong>sample</strong>: Missing response to <strong>Email address requirement</strong>.'
    ),
    'missing_value_health_safety': (
        'Sheet <strong>sample</strong>: Missing response to <strong>Health and Safety</strong> risk.'
    ),
    'missing_value_sample_return': (
        'Sheet <strong>sample</strong>: Missing response to <strong>Sample return requirement</strong>.'
    ),
    'no_lims_configured': 'No LIMS configured; nothing submitted.',
    'not_edp_profile': 'Profile {profile_title} is not an Earlham Data Profile (EDP) profile.',
    'profile_email_not_sent': (
        'An email address was not set up on this server so, an email was not sent about the created profile to:<br>'
        '{emails}'
    ),
    'profile_not_found': 'Profile <strong>{profile_title}</strong> (with ID: {profile_id}) not found.',
    'profile_owner_cannot_join': 'Profile owner cannot join the profile.',
    'profile_share_failed': (
        'Profile has been saved but, it could not be shared to the following email addresses:<br>'
        '{emails}'
        'Please check the email addresses or contact the COPO team.'
    ),
    'sample_count_below_submitted': (
        '{submitted_sample_count} of {total_sample_count} samples '
        'in Sapio project {project_id} have already been submitted so, the '
        'number of samples cannot be set lower than {submitted_sample_count}.'
    ),
    'sapio_department_not_found': "The department '{department}' is not found in Sapio.",
    'sapio_duplicate_sample_ids': (
        'Profile has been saved but, the sample creation in Sapio failed '
        'because the generated sample ID(s) already exist.\n'
        'Please contact COPO or the LIMS team to check the Sapio sample ID counter.'
    ),
    'sapio_manifest_samples_skipped': (
        'EDP data was submitted to Sapio Project {project_id} but the '
        'following sample IDs were not found in the project and were skipped: '
        '{sample_ids}'
    ),
    'sapio_manifest_submission_failed': (
        'Failed to submit manifest and attach its data to project {project_id} in Sapio: {error}'
    ),
    'sapio_manifest_submitted': 'EDP data submitted to Sapio Project {project_id} successfully.',
    'sapio_project_delete_failed': (
        'Profile has been deleted but it failed to be removed from Sapio.'
    ),
    'sapio_project_not_found': 'Sapio Project {project_id} not found.',
    'sapio_project_update_failed': 'Profile has been saved but it failed to be updated in Sapio.',
    'submission_not_found': (
        'No submission is found for the profile <strong>{profile_title}</strong> (with ID: {profile_id}) '
        ' and study {study_id}.'
    ),
    'submitter_sample_reference_changed': (
        "The 'Submitter sample reference' field of a submitted sample cannot be changed. "
        "Please restore the original 'Submitter sample reference' value of these samples: {sample_ids}."
    ),
    'user_not_authorised_to_join': 'User is not authorised to join the profile.',
}
