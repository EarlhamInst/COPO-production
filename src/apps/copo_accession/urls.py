from django.urls import path, re_path
from . import views

app_name = 'copo_profile'

urlpatterns = [
    #path('copo_visualize_accessions_dashboard', views.copo_visualize_accessions_dashboard, name="copo_visualize_accessions_dashboard"),
    path('get_filter_accession_titles', views.get_filter_accession_titles,
         name="get_filter_accession_titles"),
    path('dashboard', views.copo_accessions_dashboard, name="copo_accessions_dashboard"),
    re_path(r'^(?P<profile_id>[a-z0-9]+)/view',
            views.copo_accessions,
            name='copo_accessions'),
]