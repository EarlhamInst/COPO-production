from datetime import datetime
from django.db import models
from tinymce import models as tinymce_models
import uuid

# Create a NewsCategory model
class NewsCategory(models.Model):
    class Meta:
        verbose_name_plural = 'News Categories'

    name = models.CharField(max_length=100, blank=False, unique=True, null=False)
    description = models.TextField(max_length=200, blank=True, default=str())
    colour = models.CharField(max_length=7, blank=False, default='#ffffff')

    def __str__(self):
        return self.name
    
    def add_news_category(self, name, description=str(), colour='#' + str(uuid.uuid4().hex[:6])):
        self.name = name
        self.description = description
        self.colour = colour
        self.save()
        return self
    
    def get_all_news_categories(self):
        return NewsCategory.objects.all()
    
    # Update news category
    def update_news_category_by_name(self, old_name, new_name, new_description, new_colour):
        try:
            # Get news category by name
            self = NewsCategory.objects.get(name=old_name)

            if new_name:
                self.name = new_name

            if new_description:
                self.description = new_description

            if new_colour:
                self.colour = new_colour
            
            self.save()
        except NewsCategory.DoesNotExist as e:
            print(f'News category with name, "{old_name}", does not exist. ', str(e))
            return None
        
    def get_news_category_by_name(self, name):
        try:
            objects = NewsCategory.objects.get(name=name)
            return objects
        except NewsCategory.DoesNotExist as e:
            print(f'News category with name, "{name}", does not exist. ', str(e))
            return None
    
    def remove_news_category_by_name(self, name):
        try:
            self = NewsCategory.objects.get(name=name)
            self.delete()
            return True
        except NewsCategory.DoesNotExist as e:
            print(f'News category with name, "{name}", does not exist. ', str(e))
            return False
    
    def remove_all_news_categories(self):
        NewsCategory.objects.all().delete()
        return True

class News(models.Model):
    # This allows the model to be pluralised and appear
    # as 'News' instead of 'Newss'
    # in the Django admin interface
    # since Django classes should be singular
    class Meta:
        verbose_name_plural = 'News'
        ordering = ['-created_date']
 

    title = models.CharField(max_length=200, blank=False, unique=True, null=False)
    content = tinymce_models.HTMLField()
    # Get category from NewsCategory model as a list of choices
    category = models.ForeignKey(NewsCategory, on_delete=models.CASCADE, blank=False, null=False)
    author = models.TextField(max_length=100, blank=False, default='COPO Project Team')
    created_date = models.DateTimeField(auto_now_add=True)
    updated_date = models.DateTimeField(auto_now=True)
    is_news_article_active = models.BooleanField(default=False)
    associated_website_link = models.URLField(max_length=250, blank=True, null=True, default=str())

    def __str__(self):
        return self.title
    
    # @property
    def news_grid_colour(self):
        # Set grid colour based on the colour assigned to the category
        return self.category.colour
    
    #    CATEGORIES = [
    #     ('General', 'General'),
    #     ('Maintenance', 'Maintenance'),
    #     ('Release', 'Release'),
    #     ('Other', 'Other')
    # ]
    #  random_colour = '#' + str(uuid.uuid4().hex[:6])
    # self.CATEGORY_COLOURS.get(self.category, random_colour)
    # CATEGORY_COLOURS = {
    #     'General': '#294F85',
    #     'Maintenance': '#7BC422',
    #     'Release': '#F7005E',
    #     'Other': '#009c95'
    # }

    def add_news_article(self, title, content, author='COPO Project Team', category='General', is_news_article_active=False, associated_website_link=str()):
        self.title = title
        self.content = content
        self.author = author
        self.category = NewsCategory().get_news_category_by_name(category)
        # Get created date and updated datetime of the news article in GB timezone

        self.is_news_article_active = is_news_article_active
        self.associated_website_link = associated_website_link
        self.save()
        return self
    
    def get_all_news_articles(self):
        return News.objects.all()
    
    def get_news_article_by_id(self, id):
        try:
            objects= News.objects.get(id=id)
            return objects
        except News.DoesNotExist as e:
            print(f'News article with ID, "{id}", does not exist. ', str(e))
            return None
    
    def get_news_article_by_title(self, title):
        try:
            objects = News.objects.get(title=title)
            return objects
        except News.DoesNotExist as e:
            print(f'News article with title, "{title}", does not exist. ', str(e))
            return None
    
    def update_news_article_by_title(self, old_title, new_title, new_content, new_category, new_author, new_associated_website_link):
        try:
            # Get news article by title
            self = News.objects.get(title=old_title)

            if new_title:
                self.title = new_title

            if new_content:
                self.content = new_content

            if new_category:
                self.category = new_category

            if new_author:
                self.author = new_author

            if new_associated_website_link:
                self.associated_website_link = new_associated_website_link
            
            self.updated_date = datetime.now(datetime.UTC)
            self.save()
        except News.DoesNotExist as e:
            print(f'News article with title, "{old_title}", does not exist. ', str(e))
            return None
    
    def update_news_article_active_status_by_title(self, title, new_news_article_active_status):
        try:
            self = News.objects.get(title=title)
            self.is_news_article_active = new_news_article_active_status
            self.updated_date = datetime.now(datetime.UTC)
            self.save()
        except News.DoesNotExist as e:
            print(f'News article with title, "{title}", does not exist. ', str(e))
            return None
    
    def remove_news_article_by_title(self, title):
        try:
            self = News.objects.get(title=title)
            self.delete()
            return True
        except News.DoesNotExist as e:
            print(f'News article with title, "{title}", does not exist. ', str(e))
            return False
    
    def remove_news_article_by_id(self, id):
        try:
            self = News.objects.get(id=id)
            self.delete()
            return True
        except News.DoesNotExist as e:
            print(f'News article with ID, "{id}", does not exist. ', str(e))
            return False

    def remove_all_news_articles(self):
        News.objects.all().delete()
        return True
