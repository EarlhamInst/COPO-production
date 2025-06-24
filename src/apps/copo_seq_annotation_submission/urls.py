from django.urls import path, re_path
from . import views

app_name = 'copo_seq_annotation_submission'

urlpatterns = [
    re_path(r'^view/(?P<profile_id>[a-z0-9]+)/(?P<ui_component>\w+)', views.copo_seq_annotation,
            name='copo_seq_annotation'),
    re_path(r'^(?P<profile_id>[a-z0-9]+)/(?P<seq_annotation_id>[a-z0-9]+)', views.ena_annotation,
         name="ena_annotation"),
    re_path(r'^(?P<profile_id>[a-z0-9]+)', views.ena_annotation,
         name="ena_annotation"),

]
