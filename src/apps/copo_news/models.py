from common.utils.logger import Logger
from django.conf import settings
from django.db import models
from django.db.models.signals import post_delete, pre_save, post_save
from django.dispatch import receiver
from django.utils import timezone
from pathlib import Path
from tinymce.models import HTMLField
from .storage import OverwriteStorage
import os
import random
import shutil


lg = Logger()

def default_news_image():
    return os.path.join('news_images', 'news_image_default.jpg')

def news_image_upload_path(instance, filename):
    # Return a temporary directory path by default
    return f'news_images/temp/{filename}'

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
    news_image = models.ImageField(upload_to=news_image_upload_path, storage=OverwriteStorage(location=settings.STATIC_ROOT, base_url=settings.MEDIA_URL), blank=True, null=True)
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

    def remove_all_news_articles(self):
        # Delete all directories in the 'media/news_images/' folder
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

@receiver(post_save, sender=News)
def move_image(sender, instance, **kwargs):
    static_news_directory = os.path.join(settings.STATIC_ROOT, 'news_images')
    static_temp_directory = os.path.join(static_news_directory, 'temp')

    # Update image path and move the image to 'media/news_images/' directory 
    # from 'static/news_images/temp/' directory
    if instance.pk and instance.news_image and instance.news_image.name.startswith('news_images/temp/'):
        news_image_name = os.path.join('news_images',str(instance.pk), os.path.basename(instance.news_image.name))
        media_news_images_directory = os.path.join(settings.MEDIA_ROOT, 'news_images', str(instance.pk))
        os.makedirs(media_news_images_directory, exist_ok=True)
        shutil.move(instance.news_image.path, os.path.join(settings.MEDIA_ROOT, news_image_name))
        instance.news_image.name = news_image_name
        instance.save()

        # Clean up the 'static/news_images' directory
        shutil.rmtree(static_temp_directory, ignore_errors=True)
        shutil.rmtree(static_news_directory, ignore_errors=True)

@receiver(post_delete, sender=News)
def delete_associated_news_images_media_directory(sender, instance, **kwargs):
    news_images_directory = os.path.join('media', 'news_images', str(instance.pk))

    try:
        if os.path.exists(news_images_directory):
            shutil.rmtree(news_images_directory)
    except Exception as e:
        lg.exception(f'Error deleting directory: {news_images_directory} {str(e)}')