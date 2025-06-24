from django.urls import path, re_path
from . import views
from .utils.EnaTaggedSequence import EnaTaggedSequence

app_name = 'copo_barcoding_submission'

urlpatterns = [
    re_path(r'^view(?P<profile_id>[a-z0-9]+)/(?P<ui_component>\w+)', views.copo_taggedseq, name='copo_taggedseq'),   

    re_path(r'^ena_taggedseq_manifest_validate/(?P<profile_id>[a-z0-9]+)', views.ena_taggedseq_manifest_validate,
         name="ena_taggedseq_manifest_validate"),
    path('parse_ena_taggedseq_spreadsheet/', EnaTaggedSequence().parse_ena_taggedseq_spreadsheet,
         name="parse_ena_taggedseq_spreadsheet"),
    path('save_ena_taggedseq_records/', EnaTaggedSequence().save_ena_taggedseq_records,
         name="save_taggedseq_records"),

]
