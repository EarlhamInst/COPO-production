from django.urls import path, re_path
from . import views

app_name = 'copo_accession'

urlpatterns = [
    path('records/available', views.are_accession_records_available, name='are_accession_records_available'),
    path('get_filter_accession_titles', views.get_filter_accession_titles,
         name='get_filter_accession_titles'),
    path('generate_accession_records', views.generate_accession_records,name='generate_accession_records'),
    path('get_accession_records_column_names', views.get_accession_records_column_names,name='get_accession_records_column_names'),
    path('dashboard', views.copo_accessions_dashboard, name='copo_accessions_dashboard'),
    re_path(r'^view/(?P<profile_id>[a-z0-9]+)/(?P<ui_component>\w+)', views.copo_accessions,name='copo_accessions'),
]