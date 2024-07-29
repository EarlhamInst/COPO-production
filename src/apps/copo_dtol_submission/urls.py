from django.urls import path 

#from web.apps.web_copo.file_server import BaseFileDownloadView  deprecated
#from src.apps.web_copo.utils import annotation_handlers, template_handlers, EnaSpreadsheetParse
from . import views

app_name = 'copo_dtol_submission'

urlpatterns = [
    #path('', views.index, name='index'),

    path('accept_reject_sample/', views.copo_sample_accept_reject, name="accept_reject"),
    path('get_sample_column_names/', views.get_samples_column_names,
         name="get_sample_column_names"),         
    path('update_pending_samples_table/', views.update_pending_samples_table,
         name="update_pending_samples_table"),
    path('get_dtol_samples_for_profile/', views.get_dtol_samples_for_profile,
         name="get_dtol_samples_for_profile"),
    path('mark_sample_rejected/', views.mark_sample_rejected,
         name="mark_sample_rejected"),
    path('add_sample_to_dtol_submission/', views.add_sample_to_dtol_submission,
         name="add_sample_to_dtol_submission"),
    path('delete_dtol_samples/', views.delete_dtol_samples,
         name="delete_dtol_samples"),


]
