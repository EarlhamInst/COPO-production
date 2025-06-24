from django.urls import path, re_path
from . import views

app_name = 'copo_assembly_submission'

urlpatterns = [
    re_path(r'^view/(?P<profile_id>[a-z0-9]+)/(?P<ui_component>\w+)', views.copo_assembly,
         name='copo_assembly'),
    re_path(r'^(?P<profile_id>[a-z0-9]+)/(?P<assembly_id>[a-z0-9]+)', views.ena_assembly,
         name='ena_assembly'),
    re_path(r'^(?P<profile_id>[a-z0-9]+)', views.ena_assembly,
         name='ena_assembly'),
]
