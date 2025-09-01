from django.urls import path, re_path
from . import views

app_name = 'copo_single cell_schemas'

urlpatterns = [

    re_path('parse_singlecell_spreadsheet/(?P<profile_id>[a-z0-9]+)/(?P<schema_name>\w+)/', views.parse_singlecell_spreadsheet,
         name="parse_singlecell_spreadsheet"),
    re_path('save_singlecell_records/(?P<profile_id>[a-z0-9]+)/(?P<schema_name>\w+)/', views.save_singlecell_records,
         name="save_singlecell_records"),

    re_path("download_manifest/(?P<profile_id>[a-z0-9]+)/(?P<schema_name>\w+)/(?P<study_id>[A-Za-z0-9]+)", views.download_manifest),
    re_path("download_initial_manifest/(?P<profile_id>[a-z0-9]+)/(?P<schema_name>\w+)/(?P<checklist_id>\w+)", views.download_init_blank_manifest, name="download_initial_manifest"),

    re_path('view/(?P<profile_id>[a-z0-9]+)/(?P<schema_name>\w+)/(?P<ui_component>\w+)', views.copo_singlecell,
         name='copo_singlecell'),

    re_path('term/(?P<schema_name>\w+)/(?P<term>\w+)', views.display_term, name='display_term'),
         
]
