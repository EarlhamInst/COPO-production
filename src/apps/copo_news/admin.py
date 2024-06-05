from django.contrib import admin
from django.db import models
from .models import News
from tinymce.widgets import TinyMCE

# Register your models here
class NewsAdmin(admin.ModelAdmin):
    # list_display = ('title', 'created_at', 'updated_at')
    # list_filter = ('created_at', 'updated_at')
    # search_fields = ('title', 'content')
    formfield_overrides = {
        models.TextField: {'widget': TinyMCE()},
    }

admin.site.register(News, NewsAdmin)