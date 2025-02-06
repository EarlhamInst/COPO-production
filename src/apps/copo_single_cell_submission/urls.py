from django.urls import path, re_path
from . import views

app_name = 'copo_single cell_schemas'

urlpatterns = [
    path('singlecell_manifest_validate/<profile_id>', views.singlecell_manifest_validate,
         name="singlecell_manifest_validate"),

    path('parse_singlecell_spreadsheet/', views.parse_singlecell_spreadsheet,
         name="parse_singlecell_spreadsheet"),
    path('save_singlecell_records/', views.save_singlecell_records,
         name="save_singlecell_records"),

    
    re_path(r'^(?P<profile_id>[a-z0-9]+)/view', views.copo_singlecell,
         name='copo_singlecell'),
         
]
