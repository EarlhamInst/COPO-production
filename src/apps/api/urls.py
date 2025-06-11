__author__ = 'felix.shaw@tgac.ac.uk - 20/01/2016'

from django.urls import path, re_path

# from web.apps.web_copo import ajax_handlers
# from .annotate_views import search_all, post_annotations, handle_upload
from .views import audit, person, general, stats, profile, mapping
from .views import sample as s
from .views.sample import (
    APIValidateManifest,
    APIGetManifestValidationReport,
    APIGetUserValidations,
)
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import TemplateView
from .views import manifest_view

app_name = 'api'

generic_api_patterns = [
    re_path(r'^person/get/(?P<id>[a-z0-9]+)', person.get, name='person/get'),
    path('person/get/', person.get_all, name='person/get/all'),
    path('sample/get/', s.get_all, name='sample/get/all'),
    # path('search/', search_all, name='search_all'),
    # path('annotations/', post_annotations, name='post_annotations'),
    # path('upload_annotation_file/', handle_upload, name='handle_upload'),
    re_path(r'^stats/numbers', general.numbers, name='stats/numbers'),
]

dtol_api_patterns = [
    path('', general.forward_to_swagger),
    path('apiKey/', csrf_exempt(general.CustomAuthToken.as_view())),
    re_path(r'^sample/get/(?P<id>[A-Za-z0-9]+)', s.get, name='sample/get'),
    re_path(
        r'^audit/sample/copo_id/(?P<copo_id>[A-Za-z0-9, ]+)',
        audit.get_sample_updates_by_copo_id,
        name='get_sample_updates_by_copo_id',
    ),
    # dates must be ISO 8601 formatted
    re_path(
        r'^manifest/validations',
        APIGetUserValidations.as_view(),
        name='/manifest/validate/report',
    ),
    re_path(
        r'^manifest/validate/report',
        APIGetManifestValidationReport.as_view(),
        name='/manifest/validate/report',
    ),
    re_path(
        r'^manifest/validate', APIValidateManifest.as_view(), name='manifest/validate'
    ),
    re_path(
        r'^manifest/sequencing_centre',
        s.get_manifests_by_sequencing_centre,
        name='get_manifests_by_sequencing_centre',
    ),
    re_path(
        r'^manifest/current_version',
        s.get_current_manifest_version,
        name='get_current_manifest_version',
    ),
    re_path(
        r'^manifest/(?P<project>[a-zA-Z-_, ]+)/(?P<d_from>[A-Z0-9a-f- .:+]+)/(?P<d_to>[A-Z0-9a-f- .:+]+)',
        s.get_project_manifests_between_dates,
        name='get_project_manifests_between_dates',
    ),
    re_path(
        r'^manifest/(?P<d_from>[A-Z0-9a-f- .:+]+)/(?P<d_to>[A-Z0-9a-f- .:+]+)',
        s.get_all_manifest_between_dates,
        name='get_all_manifests_between_dates',
    ),
    re_path(
        r'^manifest/(?P<manifest_id>[A-Z0-9a-f-]+)/sample_status',
        s.get_sample_status_for_manifest,
        name='get_sample_status_for_manifest',
    ),
    re_path(
        r'^manifest/(?P<manifest_id>[A-Z0-9a-f-]+)',
        s.get_samples_in_manifest,
        name='get_for_manifest',
    ),
    re_path(r'^manifest/', s.get_manifests, name='get_manifests'),
    re_path(r'^sample/get/(?P<id>[A-Za-z0-9]+)', s.get, name='sample/get'),
    re_path(
        r'^sample/biosampleAccession/(?P<biosampleAccessions>[A-Z0-9, ]+)',
        s.get_by_biosampleAccessions,
        name='get_by_biosampleAccessions',
    ),
    re_path(
        r'^sample/copo_id/(?P<copo_ids>[A-Za-z0-9, ]+)',
        s.get_by_copo_ids,
        name='get_by_copo_ids',
    ),
    re_path(
        r'^sample/sample_field/(?P<dtol_field>[A-Za-z0-9-_]+)/(?P<value>[A-Za-z0-9-_ ,.@]+)',
        s.get_by_field,
        name='get_by_dtol_field',
    ),
    re_path(
        r'^sample/sequencing_centre',
        s.get_samples_by_sequencing_centre,
        name='get_samples_by_sequencing_centre',
    ),
    re_path(
        r'^sample/dtol/num_samples', s.get_num_dtol_samples, name='get_num_dtol_samples'
    ),
    re_path(
        r'^sample/associated_tol_project/(?P<value>[a-zA-Z-_]+)',
        s.get_project_samples_by_associated_project_type,
        name='get_project_samples_by_associated_project_type',
    ),
    re_path(
        r'^sample/project/manifest_version/fields',
        s.get_fields_by_manifest_version,
        name='get_fields_by_manifest_version',
    ),
    re_path(
        r'^sample/SampleFromStudyAccession/(?P<accessions>[A-Za-z0-9, ]+)',
        s.get_samples_from_study_accessions,
        name='get_samples_from_study_accession',
    ),
    re_path(
        r'^sample/StudyFromSampleAccession/(?P<accessions>[A-Za-z0-9, ]+)',
        s.get_study_from_sample_accession,
        name='get_study_from_sample_accession',
    ),
    re_path(
        r'^sample/updatable_fields',
        s.get_updatable_fields_by_project,
        name='get_updatable_fields_by_project',
    ),
    re_path(
        r'^sample/(?P<d_from>[A-Z0-9a-f- .:+]+)/(?P<d_to>[A-Z0-9a-f- .:+]+)',
        s.get_all_samples_between_dates,
        name='get_all_samples_between_dates',
    ),
    re_path(
        r'^sample/(?P<project>[a-zA-Z, ]+)/',
        s.get_project_samples,
        name='get_project_samples',
    ),
    re_path(
        r'^audit/sample/asg',
        audit.get_asg_sample_updates_by_updatable_field,
        name='get_asg_sample_updates_by_updatable_field',
    ),
    re_path(
        r'^audit/sample/dtol',
        audit.get_dtol_sample_updates_by_updatable_field,
        name='get_dtol_sample_updates_by_updatable_field',
    ),
    re_path(
        r'^audit/sample/erga',
        audit.get_erga_sample_updates_by_updatable_field,
        name='get_erga_sample_updates_by_updatable_field',
    ),
    re_path(
        r'^audit/sample/update_type/(?P<update_type>[a-zA-Z, ]+)',
        audit.get_sample_updates_by_update_type,
        name='get_sample_updates_by_update_type',
    ),
    re_path(
        r'^audit/sample/manifest_id/(?P<manifest_id>[A-Z0-9a-f-, ]+)',
        audit.get_sample_updates_by_manifest_id,
        name='get_sample_updates_by_manifest_id',
    ),
    re_path(
        r'^audit/sample/(?P<d_from>[A-Z0-9a-f- .:+]+)/(?P<d_to>[A-Z0-9a-f- .:+]+)',
        audit.get_sample_updates_between_dates,
        name='get_sample_updates_between_dates',
    ),
    re_path(
        r'^audit/sample/(?P<field>[A-Za-z0-9-_]+)/(?P<field_value>[A-Za-z0-9-_ ,.@]+)',
        audit.get_sample_updates_by_sample_field_and_value,
        name='get_sample_updates_by_sample_field_and_value',
    ),
    re_path(
        r'^profile/make_profile',
        profile.APICreateProfile.as_view(),
        name='make_profile',
    ),
    re_path(
        r'^profile/get_for_user',
        profile.APIGetProfilesForUser.as_view(),
        name='get_for_user',
    ),
    re_path(
        r'^profile/tube_or_well_ids',
        profile.associate_profiles_with_tubes_or_well_ids,
        name='associate_profiles_with_tubes_or_well_ids',
    ),
]

stats_api_patterns = [
    re_path(
        r'^stats/number_of_users', stats.get_number_of_users, name='get_number_of_users'
    ),
    re_path(
        r'^stats/number_of_samples/(?P<sample_type>[a-zA-Z-_, ]+)',
        stats.get_number_of_samples_by_sample_type,
        name='get_number_of_samples_by_sample_type',
    ),
    re_path(
        r'^stats/number_of_samples',
        stats.get_number_of_samples,
        name='get_number_of_samples',
    ),
    re_path(
        r'^stats/number_of_profiles',
        stats.get_number_of_profiles,
        name='get_number_of_profiles',
    ),
    re_path(
        r'^stats/number_of_datafiles',
        stats.get_number_of_datafiles,
        name='get_number_of_datafiles',
    ),
    re_path(
        r'^stats/combined_stats_json',
        stats.combined_stats_json,
        name='combined_stats_json',
    ),
    re_path(
        r'^stats/sample_stats_csv', stats.samples_stats_csv, name='samples_stats_csv'
    ),
    re_path(
        r'^stats/histogram_metric/(?P<metric>[A-Za-z0-9_]+)',
        stats.samples_hist_json,
        name='samples_hist_json',
    ),
    re_path(r'^stats/tol_projects', stats.get_tol_projects, name='get_tol_projects'),
    re_path(
        r'^stats/associated_tol_projects',
        stats.get_associated_tol_projects,
        name='get_associated_tol_projects',
    ),
]

mapping_api_patterns = [re_path(r'^mapping', mapping.get_mapping, name='get_mapping')]

manifest_patterns = [
    path(
        'populate_manifest_table',
        manifest_view.populate_manifest_table,
        name="populate_manifest_table",
    ),
    path(
        'get_manifest_fields',
        manifest_view.get_manifest_fields,
        name="get_manifest_fields",
    ),
    re_path(
        r'get_manifest_file_name/(?P<manifest_type>[a-zA-Z-_, ]+)',
        manifest_view.get_manifest_file_name,
        name="get_manifest_file_name",
    ),
    path(
        'get_common_value_dropdown_list',
        manifest_view.get_common_value_dropdown_list,
        name="get_common_value_dropdown_list",
    ),
    path(
        'prefill_manifest_template/',
        manifest_view.prefill_manifest_template,
        name="prefill_manifest_template",
    ),
    re_path(
        f'download_manifest/blank/(?P<schema_name>\w+)/(?P<checklist_id>\w+)$',
        manifest_view.download_blank_manifest,
        name="download_blank_manifest",
    ),
    re_path(
        r'^download_manifest/(?P<manifest_id>[A-Z0-9a-f-]+)',
        manifest_view.download_manifest,
        name="download_manifest",
    ),
    path('download_permits/', manifest_view.download_permits, name="download_permits"),
    path('view_images/', manifest_view.view_images, name="view_images"),
    path(
        'validate_common_value',
        manifest_view.validate_common_value,
        name="validate_common_value",
    ),
    path('index', manifest_view.copo_manifests, name='manifests'),
]

urlpatterns = (
    generic_api_patterns
    + dtol_api_patterns
    + stats_api_patterns
    + mapping_api_patterns
    + manifest_patterns
)
