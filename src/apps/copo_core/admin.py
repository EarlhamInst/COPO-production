from django.contrib import admin
from django.db import models
from django_json_widget.widgets import JSONEditorWidget

from src.apps.copo_core.models import (
    UserDetails,
    ViewLock,
    Banner,
    SequencingCentre,
    AssociatedProfileType,
    ProfileType,
    Component,
    TitleButton,
    RecordActionButton,
    TourProgress,
)

class ComponentAdmin(admin.ModelAdmin):
    formfield_overrides = {
        models.JSONField: {'widget': JSONEditorWidget},
    }


class TourProgressAdmin(admin.ModelAdmin):
    list_display = ('user', 'component', 'stage', 'updated_at')
    list_filter = ('component', 'stage')


admin.site.register(UserDetails)
admin.site.register(ViewLock)
admin.site.register(Banner)
admin.site.register(SequencingCentre)
admin.site.register(ProfileType)
admin.site.register(Component, ComponentAdmin)
admin.site.register(TitleButton)
admin.site.register(RecordActionButton)
admin.site.register(TourProgress, TourProgressAdmin)

admin.site.register(AssociatedProfileType)
