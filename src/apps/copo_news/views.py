from common.utils.helpers import get_group_membership_asString
from common.utils.logger import Logger
from django.core.paginator import Paginator
from django.shortcuts import render
from .models import News

l = Logger()

def copo_news(request):
    context = dict()
    # Groups
    member_groups = get_group_membership_asString()
    # News
    news_list = News.objects.filter(is_news_article_active=True).order_by('-created_date')
    
    context = {'user': request.user, 'groups': member_groups}

    paginator = Paginator(news_list, 10)  # Show 10 news articles per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context['page_obj'] = page_obj

    return render(request, 'copo/news.html', context)