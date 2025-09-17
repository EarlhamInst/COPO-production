from django.urls import path, re_path
from . import views

app_name = 'copo_accessions_schema'

urlpatterns = [
    re_path(r'^view/(?P<profile_id>[a-z0-9]+)/(?P<ui_component>\w+)', views.copo_accessions_schema,name='copo_accessions_schema'),
]