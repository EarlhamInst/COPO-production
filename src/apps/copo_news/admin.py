from django.contrib import admin
from django.db import models
from .models import News, NewsCategory
from tinymce.widgets import TinyMCE

# Register your models here
class NewsAdmin(admin.ModelAdmin):
    formfield_overrides = {
        models.TextField: {'widget': TinyMCE()},
    }

admin.site.register(News, NewsAdmin)
admin.site.register(NewsCategory)