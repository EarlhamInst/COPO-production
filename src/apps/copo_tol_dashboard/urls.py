from django.urls import path, re_path
from . import views

app_name = 'copo_tol_dashboard'

urlpatterns = [
    path('tol_inspect/gal', views.copo_tol_inspect_gal, name="tol_inspect_gal"),
    path('tol_inspect/', views.copo_tol_inspect, name="tol_inspect"),
    path('tol', views.copo_tol_dashboard, name="copo_tol_dashboard"),
    path('tol/gal_and_partners', views.gal_and_partners, name='copo_gal_and_partners'),
    path('get_profiles_for_tol_inspection/',
         views.get_profiles_for_tol_inspection,
         name="get_profiles_for_tol_inspection"),
    path('get_profiles_based_on_sample_data/',
         views.get_profiles_based_on_sample_data,
         name="get_profiles_based_on_sample_data"),
    path('get_profile_titles_nav_tabs/',
         views.get_profile_titles_nav_tabs,
         name="get_profile_titles_nav_tabs"),
    path('get_gal_names/', views.get_gal_names, name="get_gal_names"),
    path('get_samples_by_search_faceting/', views.get_samples_by_search_faceting,
         name="get_samples_by_search_faceting"),
    path('get_sample_details/', views.get_sample_details,
         name="get_sample_details"),   
    path('stats/<str:view>', views.stats, name='stats'),
    path('stats/', views.stats, name='stats'),            
]