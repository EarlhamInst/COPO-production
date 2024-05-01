from django.urls import path, re_path
from . import views

app_name = 'copo_read_submission'

urlpatterns = [
    path('ena_read_manifest_validate/<profile_id>', views.ena_read_manifest_validate,
         name="ena_read_manifest_validate"),

    path('parse_ena_spreadsheet/', views.parse_ena_spreadsheet,
         name="parse_ena_spreadsheet"),
    path('save_ena_records/', views.save_ena_records,
         name="save_ena_records"),
    path('get_manifest_submission_list/', views.get_manifest_submission_list,
         name="get_manifest_submission_list"),
    path('init_manifest_submission/', views.init_manifest_submission,
         name="init_manifest_submission"),
    path('get_submission_status/', views.get_submission_status, 
         name="get_submission_status"),
    re_path(r'^(?P<sample_accession>[A-Z0-9]+)/get_read_accessions', views.get_read_accessions,
         name='get_read_accessions'),
    re_path(r'^(?P<profile_id>[a-z0-9]+)/view', views.copo_reads,
         name='copo_reads'),
    re_path(r'^(?P<profile_id>[a-z0-9]+)/download_initial_read_manifest', views.download_initial_read_manifest, name='download_initial_manifest'),     
         
]
