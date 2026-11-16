from django.urls import path, re_path

from . import views

app_name = 'ei_edp'

urlpatterns = [
    path('sample_batch_options/', views.get_sample_batch_options, name='sample_batch_options'),
    re_path(r'^(?P<profile_id>[a-z0-9]+)/sample_batches', views.sample_batches, name='sample_batches'),
]
