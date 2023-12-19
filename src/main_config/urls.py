from django.conf import settings
from django.contrib import admin
from django.urls import include, path, re_path
from django.views.generic import TemplateView
from django.conf.urls.static import static

import src.apps.copo_core.views as views
from src.apps.copo_landing_page import views as landing_views

admin.autodiscover()

urlpatterns = [
    path('admin/', admin.site.urls),
    path('copo/copo_sample/', include('src.apps.copo_sample.urls', namespace='copo_sample')),
    path('copo/dtol_manifest/', include('src.apps.copo_dtol_upload.urls', namespace='copo_dtol_upload')),
    path('copo/dtol_submission/', include('src.apps.copo_dtol_submission.urls', namespace='copo_dtol_submission')),
    path('copo/copo_read/', include('src.apps.copo_read_submission.urls', namespace='copo_read_submission')),
    path('copo/copo_assembly/', include('src.apps.copo_assembly_submission.urls', namespace='copo_assembly_submission')),
    path('copo/copo_seq_annotation/', include('src.apps.copo_seq_annotation_submission.urls', namespace='copo_seq_annotation_submission')),
    path('copo/copo_taggedseq/', include('src.apps.copo_barcoding_submission.urls', namespace='copo_barcoding_submission')),
    path('copo/copo_files/', include('src.apps.copo_file.urls', namespace='copo_file')),
    path('copo/auth/', include('src.apps.copo_login.urls', namespace='copo_login')),
    re_path(r'^copo/', include('src.apps.copo_profile.urls', namespace='copo_profile_index')),
    path('copo/copo_profile/', include('src.apps.copo_profile.urls', namespace='copo_profile')),
    path('accounts/profile/', include('src.apps.copo_profile.urls', namespace='copo_account_profile_index')),
    path('copo/copo_accessions/', include('src.apps.copo_accession.urls', namespace='copo_accession')),
    path('copo/tol_dashboard/', include('src.apps.copo_tol_dashboard.urls', namespace='copo_tol_dashboard')),
    path('copo/', include('src.apps.copo_core.urls', namespace='copo')),
    #path('rest/', include('src.apps.copo_core.rest_urls', namespace='rest')),

    path('api/', include('src.apps.api.urls', namespace='api')),
    path('manifests/', include('src.apps.api.urls', namespace='manifests')),
    path('accounts/', include('allauth.urls')),
   
    path('', landing_views.index, name='index'),
    path('cookie_response/', landing_views.cookie_response, name='cookie_response'),
    path('is_user_email_address_provided/', landing_views.is_user_email_address_provided, name='is_user_email_address_provided'),
    path('about/', TemplateView.as_view(template_name="about.html"), name='about'),
    path('about/privacy_notice/', TemplateView.as_view(template_name="privacy_notice.html"),
         name='privacy_notice'),
    path('about/terms_of_use/', TemplateView.as_view(template_name="terms_of_use.html"),
         name='terms_of_use'),
    path('people/', TemplateView.as_view(template_name="people.html"), name='people'),
    path('dtol/', TemplateView.as_view(template_name="dtol.html"), name='dtol'),
    path('news/', TemplateView.as_view(template_name="news.html"), name='news'),
    path('ebp/', TemplateView.as_view(template_name="ebp_resources.html"), name="ebp"),

]

#handler404 = views.handler404
handler500 = views.handler500
handler403 = views.handler403

#if settings.DEBUG is False:  # if DEBUG is True it will be served automatically
if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

