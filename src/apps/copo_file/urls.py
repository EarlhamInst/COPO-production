from django.urls import path, re_path
from . import views

app_name = 'copo_file'

urlpatterns = [
    re_path(r'^view/(?P<profile_id>[a-z0-9]+)/(?P<ui_component>\w+)', views.copo_files,
            name='copo_files'),     
    re_path(r'^upload_ecs_files/(?P<profile_id>[a-z0-9]+)', views.upload_ecs_files, name='upload_ecs_files'),      
    path('process_urls', views.process_urls,
         name="process_urls"),
]
