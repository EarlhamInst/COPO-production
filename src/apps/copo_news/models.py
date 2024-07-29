from common.utils.logger import Logger

from datetime import datetime

from django.core.files import File
from django.core.files.storage import default_storage
from django.db import models
from django.db.models.signals import post_delete, pre_save, post_save
from django.dispatch import receiver
from django.utils import timezone
from pathlib import Path
from tinymce.models import HTMLField
from .storage import OverwriteStorage
import os
import random
import uuid
import shutil

lg = Logger()

def default_news_image():
    return os.path.join('news_images', 'news_image_default.jpg')

def news_image_upload_path(instance, filename):
    # Return a temporary directory path by default
    return f'temp/{filename}'

def delete_news_images_directory_content():
    media_directory = os.path.join('media', 'news_images')
    
    try:
        if os.path.exists(media_directory):
            shutil.rmtree(media_directory)
            lg.log(f'Successfully deleted directory: {media_directory}')
        else:
            lg.log(f'Directory does not exist: {media_directory}')
    except Exception as e:
        lg.exception(f'Error deleting directory content: {media_directory}:  {str(e)}')

class NewsCategory(models.Model):
    class Meta:
        verbose_name_plural = 'News Categories'

    name = models.CharField(max_length=100, blank=False, unique=True, null=False)
    description = models.TextField(max_length=200, blank=True, default=str())
    colour = models.CharField(max_length=7, blank=False, default='#ffffff')

    def __str__(self):
        return self.name
    
    def add_news_category(self, name, description='', colour=None):
        if colour is None:
            colour = '#' + uuid.uuid4().hex[:6]

        self.name = name
        self.description = description
        self.colour = colour
        self.save()
        return self
    
    def get_all_news_categories(self):
        return NewsCategory.objects.all()
    
    def update_news_category_by_name(self, old_name, new_name=None, new_description=None, new_colour=None):
        try:
            category = NewsCategory.objects.get(name=old_name)
            if new_name:
                category.name = new_name
            if new_description:
                category.description = new_description
            if new_colour:
                category.colour = new_colour
            category.save()
            return category
        except NewsCategory.DoesNotExist as e:
            lg.exception(f'News category with name, "{old_name}", does not exist. ', str(e))
            return None
        
    def get_news_category_by_name(self, name):
        try:
            return NewsCategory.objects.get(name=name)
        except NewsCategory.DoesNotExist as e:
            lg.exception(f'News category with name, "{name}", does not exist. ', str(e))
            return None

    
    def remove_news_category_by_name(self, name):
        try:
            category = NewsCategory.objects.get(name=name)
            category.delete()
            return True
        except NewsCategory.DoesNotExist as e:
            lg.exception(f'News category with name, "{name}", does not exist. ', str(e))
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
    content = HTMLField()
    category = models.ForeignKey(NewsCategory, on_delete=models.CASCADE, blank=False, null=False)
    author = models.CharField(max_length=200, blank=False, default='COPO Project Team')
    
    # Image will be uploaded to 'media/news_images' directory
    news_image = models.ImageField(upload_to=news_image_upload_path, storage=OverwriteStorage(), blank=True, null=True)
    created_date = models.DateTimeField(default=timezone.now, editable=False)
    updated_date = models.DateTimeField(auto_now=True)
    is_news_article_active = models.BooleanField(default=True)

    def __str__(self):
        return self.title
    
    def save(self, *args, **kwargs):
        if not self.id:
            self.created_at = timezone.now()
        self.updated_at = timezone.now()
        super(News, self).save(*args, **kwargs)

    def set_news_images_directory_permissions(self):
        news_images_directory = os.path.join('media', 'news_images')

        try:
            if os.path.exists(news_images_directory):
                # try:
                #     subprocess.run(['chmod', '-R', '775', news_images_directory], check=True)
                # except subprocess.CalledProcessError as e:
                #     print(f'Error: {e}')
                os.chmod(news_images_directory, 0o775)
            else:
                self.create_news_images_directory()
        except Exception as e:
            lg.exception(f'Error changing directory permissions: {news_images_directory} {str(e)}')

    # Get related news items within the same category, excluding the current news item
    def get_related_news(self):
        # Show 6 related news items each time
        news_items_count = 6
        related_news = list(News.objects.filter(category=self.category).exclude(id=self.id))
        random.shuffle(related_news)
        return related_news[:news_items_count]
    
    # Get next and previous news item based on news ID
    def get_next_and_previous_news(self):
        previous_news = News.objects.filter(id__lt=self.id).order_by('-id').first()
        next_news = News.objects.filter(id__gt=self.id).order_by('id').first()
        return previous_news, next_news

    def add_news_article(self, title, content,  category='General', author='COPO Project Team', news_image=default_news_image(), is_news_article_active=False):
        self.title = title
        self.content = content
        self.category = NewsCategory().get_news_category_by_name(category)
        self.author = author
        self.news_image = news_image
        self.is_news_article_active = is_news_article_active
        self.save()
        return self
    
    def get_all_news_articles(self):
        return News.objects.all()
    
    def get_news_article_by_id(self, id):
        try:
            objects= News.objects.get(id=id)
            return objects
        except News.DoesNotExist as e:
            lg.exception(f'News article with ID, "{id}", does not exist. ', str(e))
            return None
    
    def get_news_article_by_title(self, title):
        try:
            objects = News.objects.get(title=title)
            return objects
        except News.DoesNotExist as e:
            lg.exception(f'News article with title, "{title}", does not exist. ', str(e))
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
            lg.exception(f'News article with title, "{old_title}", does not exist. ', str(e))
            return None
    
    def update_news_article_active_status_by_title(self, title, new_news_article_active_status):
        try:
            self = News.objects.get(title=title)
            self.is_news_article_active = new_news_article_active_status
            self.updated_date = datetime.now(datetime.UTC)
            self.save()
        except News.DoesNotExist as e:
            lg.exception(f'News article with title, "{title}", does not exist. ', str(e))
            return None
    
    def remove_news_article_by_title(self, title):
        try:
            self = News.objects.get(title=title)
            self.delete_news_images_directory_content()
            self.delete()
            return True
        except News.DoesNotExist as e:
            lg.exception(f'News article with title, "{title}", does not exist. ', str(e))
            return False
    
    def remove_news_article_by_id(self, id):
        try:
            self = News.objects.get(id=id)
            self.delete_news_images_directory_content()
            self.delete()
            return True
        except News.DoesNotExist as e:
            lg.exception(f'News article with ID, "{id}", does not exist. ', str(e))
            return False

    def remove_all_news_articles(self):
        # Delete all directories in the 'media/news_images' folder
        News.objects.all().delete()
        delete_news_images_directory_content()
        return True
    
    def remove_unwanted_news_images(self):
        news_objects = News.objects.all()
        
        # If the directory exists, check if the incoming image already exists in it, 
        # if it does not exist, remove all other files (if any exists) then copy 
        # the incoming image into the directory
        try: 
            for news in news_objects:
                news_images_directory = os.path.join('media', 'news_images', str(news.pk))
                if os.path.exists(news_images_directory):
                    if os.listdir(os.path.join(news_images_directory)):
                        for file in os.listdir(os.path.join(news_images_directory)):
                            if file != os.path.basename(news.news_image.name):
                                os.remove(os.path.join(news_images_directory, file))
                else:
                    lg.error(f'News images directory does not exist for news article with ID: {news.pk}')
        except Exception as e:
            lg.exception(f'Error deleting unwanted news images: {str(e)}')

@receiver(post_delete, sender=News)
def delete_associated_news_images_media_directory(sender, instance, **kwargs):
    news_images_directory = os.path.join('media', 'news_images', str(instance.pk))

    try:
        if os.path.exists(news_images_directory):
            shutil.rmtree(news_images_directory)
    except Exception as e:
        lg.exception(f'Error deleting directory: {news_images_directory} {str(e)}')