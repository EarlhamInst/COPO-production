from django.contrib import admin

from src.apps.copo_core.models import UserDetails, ViewLock, Banner,\
      SequencingCentre, AssociatedProfileType, ProfileType, Component, TitleButton, RecordActionButton

admin.site.register(UserDetails)
admin.site.register(ViewLock)
admin.site.register(Banner)
admin.site.register(SequencingCentre)
admin.site.register(ProfileType)
admin.site.register(Component)
admin.site.register(TitleButton)
admin.site.register(RecordActionButton)


admin.site.register(AssociatedProfileType)
