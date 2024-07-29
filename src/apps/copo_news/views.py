from common.utils.helpers import get_group_membership_asString
from common.utils.logger import Logger
from django.core.paginator import Paginator
from django.http import Http404, JsonResponse
from django.shortcuts import render, redirect
from .models import News, NewsCategory

lg = Logger()

def news_error_handler(request, exception):
    lg.error(f'Error in news app: {str(exception)}')
    error_message = 'We could not find the news article that you are looking for. ' + \
                    'Perhaps, it might have been removed, or the URL might be incorrect.'
    return render(request, 'copo/news_error_page.html', {'message': error_message})

def get_object_or_404(model, request, **kwargs):
    try:
        return model.objects.get(**kwargs)
    except model.DoesNotExist:
        exception = f'No {model._meta.object_name} matches the given query'
        return news_error_handler(request, exception)
    except Exception as e:
        exception = f'An error occurred while retrieving the {model._meta.object_name}: {str(e)}'
        return news_error_handler(request, exception)
    
def news_list(request):
    # Show 3 news items each time
    news_items_count = 3
    news_items = News.objects.filter(is_news_article_active=True).order_by('-created_date')
    news_items_total_count = News.objects.filter(is_news_article_active=True).count()

    # Get news items based on category if the category is provided in the URL
    category = request.GET.get('category', None)

    if category:
        news_items = news_items.filter(category__name=category.title(), is_news_article_active=True)
        news_items_total_count = news_items.count()

    # Get all category names from all the news items
    news_item_categories = [NewsCategory.objects.get(id=news_item.category_id).name for news_item in news_items]
    # Remove duplicates from the list of categories
    news_item_categories = list(set(news_item_categories))
    # Sort the list of categories in ascending order
    news_item_categories.sort()
    
    output = dict()
    output['allCategories'] = news_item_categories
    output['newsItemsTotalCount'] = news_items_total_count

    # Check if the URL matches - '/news/' or '/news', 
    # if it does, redirect to the news list section on the landing 
    # or frontpage web page i.e. the URL, '/#news-section'
    # NB: 'output' is also known as 'context
    if request.path == '/news/' or request.path == '/news':
        output['newsList'] = news_items
        return render(request, 'copo/news_list.html', output) #redirect('/#news-section')
    else:
        output['newsList'] =  news_items[:news_items_count]
        return output

def news_item(request, id):
    context = dict()

    try:
        news_items = get_object_or_404(News, request, id=id)
        related_news_items = news_items.get_related_news()
        previous_news, next_news = news_items.get_next_and_previous_news()

        context['news'] = news_items
        context['relatedNews'] = related_news_items
        context['previousNews'] = next_news # The order is reversed here for UI purposes
        context['nextNews'] = previous_news # The order is reversed here for UI purposes

        return render(request, 'copo/news_item.html', context)
    except Http404:
        exception = f'No news item matches the given query'
        return news_error_handler(request, exception)
    except Exception as e:
        exception = f'An error occurred while retrieving the news item: {str(e)}'
        return news_error_handler(request, exception)

    