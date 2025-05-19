from django.urls import path, re_path
from . import views

app_name = 'copo_sample'

urlpatterns = [
    path('sample_manifest_validate/<profile_id>', views.sample_manifest_validate,
         name="sample_manifest_validate"),

    re_path(r'^(?P<profile_id>[a-z0-9]+)/view', views.copo_samples,
            name='copo_samples'),

    re_path(r'^general/(?P<profile_id>[a-z0-9]+)/view', views.copo_general_samples,
        name='copo_general_samples'),

    re_path("download_manifest/(?P<profile_id>[a-z0-9]+)/(?P<sample_checklist_id>[A-Za-z0-9]+)", views.download_manifest),
    path('parse_sample_spreadsheet/', views.parse_sample_spreadsheet,
         name="parse_sample_spreadsheet"),
    path('save_sample_records/', views.save_sample_records,
         name="save_sample_records"),

]    