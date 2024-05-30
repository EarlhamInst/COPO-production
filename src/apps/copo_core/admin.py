from django.contrib import admin

from src.apps.copo_core.models import UserDetails, ViewLock, banner_view,\
      SequencingCentre, ProfileType, Component, ActionButton, SidebarPanel

admin.site.register(UserDetails)
admin.site.register(ViewLock)
admin.site.register(banner_view)
admin.site.register(SequencingCentre)
admin.site.register(ProfileType)
admin.site.register(Component)
admin.site.register(ActionButton)
admin.site.register(SidebarPanel)


