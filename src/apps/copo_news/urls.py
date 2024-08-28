from django.urls import path
from django.shortcuts import redirect
from . import views

app_name = 'news'

urlpatterns = [
    path('', views.news_list, name='news_list'),
    # path('load_more_news', views.load_more_news, name='load_more_news'),
    path('<int:id>', views.news_item, name='news_item')
]
