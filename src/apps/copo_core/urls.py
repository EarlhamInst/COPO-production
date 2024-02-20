from django.urls import path, re_path
from . import views

app_name = 'copo_core'

urlpatterns = [

    #path('login/', views.login, name='auth'),
    #path('logout/', views.copo_logout, name='logout'),
    #path('register/', views.copo_register, name='register'),
    path('profile/update_counts/', views.get_profile_counts, name='update_counts'),
    path('view_user_info/', views.view_user_info, name='view_user_info'),
    path('error/', views.goto_error, name='error_page'),

    #re_path(r'^copo_samples/(?P<profile_id>[a-z0-9]+)/view', views.copo_samples,
    #        name='copo_samples'),
    re_path(r'^copo_submissions/(?P<profile_id>[a-z0-9]+)/view', views.copo_submissions,
            name='copo_submissions'),
    re_path(r'^resolve/(?P<submission_id>[a-z0-9]+)', views.resolve_submission_id,
            name="resolve_submission_id"),
    path('get_source_count/', views.get_source_count,
         name="get_source_count"),
    re_path(r'^ajax_search_copo_local/(?P<data_source>[a-zA-Z0-9,_]+)/$',
            views.search_copo_components, name='ajax_search_copo_local'),
    path('copo_forms/', views.copo_forms, name="copo_forms"),
    #path('delete_profile/', views.delete_profile, name="delete_profile"),
    path('copo_visualize/', views.copo_visualize, name="copo_visualize"),
    path('copo_visualize_accessions/', views.copo_visualize_accessions, name="copo_visualize_accessions"),
    path('groups/', views.view_groups, name='groups'),
    path('create_group/', views.create_group, name='create_group'),
    path('edit_group/', views.edit_group, name='edit_group'),
    path('delete_group/', views.delete_group, name='delete_group'),
    path('view_group/', views.view_group, name='view_group'),
    path('add_profile_to_group/', views.add_profile_to_group, name='add_profile_to_group'),
    path('remove_profile_from_group/', views.remove_profile_from_group,
         name='remove_profile_from_group'),
    path('get_profiles_in_group/', views.get_profiles_in_group,
         name='get_profiles_in_group'),
    path('get_users_in_group/', views.get_users_in_group, name="add_users_in_group"),
    path('get_users/', views.get_users, name="get_users"),

    path('add_user_to_group/', views.add_user_to_group, name="add_user_to_group"),
    path('remove_user_from_group/', views.remove_user_from_group, name="remove_user_from_group"),

]
