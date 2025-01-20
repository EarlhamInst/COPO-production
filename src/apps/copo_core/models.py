from datetime import datetime, timedelta
from django.db import transaction
import pytz
from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.db.models import JSONField
from django.db.models.signals import post_save
from django.dispatch import receiver
from django_tools.middlewares.ThreadLocal import get_current_user
from django.conf import settings
from django.utils import timezone
from rest_framework.authtoken.models import Token
from asgiref.sync import sync_to_async
from django.utils.translation import gettext_lazy as _
from common.dal.copo_base_da import DataSchemas


class UserDetails(models.Model):
    # This allows the model to be pluralised
    # as 'UserDetails' instead of 'UserDetailss'
    # in the Django admin interface
    # since Django classes should be singular
    class Meta:
        verbose_name_plural = 'UserDetails'

    user = models.OneToOneField(User, on_delete=models.CASCADE)
    orcid_id = models.TextField(max_length=40, blank=True)
    repo_manager = ArrayField(
        models.CharField(max_length=100, blank=True),
        blank=True,
        null=True,
    )
    repo_submitter = ArrayField(
        models.CharField(max_length=100, blank=True),
        blank=True,
        null=True,
    )
    active_task = models.BooleanField(default=False)
    cookie_consent_log = ArrayField(
        JSONField(default=dict),
        blank=True,
        null=True)
    # class Meta:
    # app_label = 'django.contrib.auth'

    def __str__(self):
        return self.user.username


@receiver(post_save, sender=User)
def create_user_profile(sender, instance, created, **kwargs):
    if created:
        UserDetails.objects.create(user=instance)
    try:
        ud = instance.userdetails
    except:
        UserDetails.objects.create(user=instance)


@receiver(post_save, sender=User)
def create_auth_token(sender, instance, **kwargs):
    try:
        with transaction.atomic():
            Token.objects.create(user=instance)
    except:
        pass


@receiver(post_save, sender=User)
def save_user_details(sender, instance, **kwargs):
    instance.userdetails.save()


class Repository(models.Model):
    class Meta:
        managed = False  # No database table creation or deletion operations \
        # will be performed for this model.

        permissions = (
            ('customer_rigths', 'Global customer rights'),
            ('vendor_rights', 'Global vendor rights'),
            ('any_rights', 'Global any rights'),
        )


class StatusMessage(models.Model):
    message_owner = models.ForeignKey(User, on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)
    message = models.TextField(
        max_length=500, blank=False, default="All Tasks Complete")

    class Meta:
        get_latest_by = 'created'


class Banner(models.Model):
    header = models.TextField(max_length=78, blank=False, default="")
    body = models.TextField(max_length=2000, blank=True, default="")
    active = models.BooleanField(default=False)


class ViewLock(models.Model):
    url = models.URLField(max_length=2000)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    timeLocked = models.DateTimeField()
    timeout = timedelta(seconds=300)

    def lockView(self, url):
        # method will throw error if lock already exists for url
        self.url = url
        self.user = get_current_user()
        self.timeLocked = datetime.utcnow()
        try:
            self.save()
            return True
        except ViewLock.MultipleObjectsReturned:
            return False

    def delete_self(self):
        self.delete()
        return True

    def unlockView(self, url):
        lock = ViewLock.objects.get(url=url)
        if lock:
            lock.delete()
            return True
        else:
            return False

    def isViewLockedCreate(self, url):
        # if this view is locked return True, if not, create lock and return False
        try:
            lock = ViewLock.objects.filter(url=url).get()
        except ViewLock.DoesNotExist as e:
            # view not locked
            self.lockView(url=url)
            return False
        if lock.user == get_current_user():
            # lock is owned by page requester, update timeLocked
            lock.timeLocked = datetime.utcnow()
            lock.save()
            return False
        else:
            if datetime.utcnow().replace(tzinfo=pytz.utc) - lock.timeLocked > self.timeout:
                # lock has expired
                lock.delete()
                self.lockView(url=url)
                return False
            else:
                # view is locked
                return True

    @sync_to_async
    def remove_expired_locks(self):
        time_threshold = timezone.now() - settings.VIEWLOCK_TIMEOUT
        locks = ViewLock.objects.filter(timeLocked__lte=time_threshold)
        for l in locks:
            l.delete()
        print(locks)


class SequencingCentre(models.Model):
    users = models.ManyToManyField(User)
    description = models.CharField(max_length=100)
    name = models.CharField(max_length=100)
    label = models.CharField(max_length=100)
    contact_details = models.CharField(
        max_length=800,
        blank=True,
        null=True)
    is_approval_required = models.BooleanField(default=False)

    def __str__(self):
        return self.name

    def create_sequencing_centre(self, name, description, label, contact_details=str(), is_approval_required=False):
        self.name = name
        self.description = description
        self.label = label
        self.contact_details = contact_details
        self.is_approval_required = is_approval_required
        self.save()
        return self

    def remove_all_sequencing_centres(self):
        SequencingCentre.objects.all().delete()
        return True

    def get_sequencing_centres(self):
        return SequencingCentre.objects.all()    

class AssociatedProfileType(models.Model):
    users = models.ManyToManyField(User)
    name = models.CharField(max_length=100)
    label = models.CharField(max_length=100)
    is_approval_required = models.BooleanField(default=False)
    is_approval_required_for_updated_manifest = models.BooleanField(default=True)
    is_acceptance_email_notification_required = models.BooleanField(default=False)
    acceptance_email_body = models.TextField(blank=True, null=True)

    def __str__(self):
        return self.name

    def create_associated_profile_type(self, name, label, is_approval_required=False, is_acceptance_email_notification_required=False, acceptance_email_body="", is_approval_required_for_updated_manifest=True):
        self.name = name
        self.label = label
        self.is_approval_required = is_approval_required
        self.is_acceptance_email_notification_required = is_acceptance_email_notification_required
        self.acceptance_email_body = acceptance_email_body
        self.is_approval_required_for_updated_manifest = is_approval_required_for_updated_manifest
        self.save()
        return self

    def remove_all_associated_profile_types(self):
        AssociatedProfileType.objects.all().delete()
        return True

    def get_associated_profile_types(self):
        return AssociatedProfileType.objects.all()


class TitleButton(models.Model):
    class Meta:
        ordering = ['name']

    name = models.CharField(max_length=50, unique=True)
    template = models.CharField(max_length=500)
    additional_attr = models.CharField(max_length=500, blank=True, null=True, help_text="Additional attributes for the button,format: key1:value1,key2:value2,key3:value3")

    def __str__(self):
        return self.name
    
    def create_title_button(self, name, template, additional_attr):
        self.name = name
        self.template = template
        self.additional_attr = additional_attr
        self.save()
        return self
    
    def remove_all_title_buttons(self):
        TitleButton.objects.all().delete()
        return True
    
    def get_title_buttons(self):
        return TitleButton.objects.all()    

class RecordActionButton(models.Model):
    class action_types(models.TextChoices):
        SINGLE = 'single', _("Single")
        MULTIPLE = 'multi', _("Multiple")
        def __str__(self):
            return self.value

    name = models.CharField(max_length=100, unique=True)
    icon_colour = models.CharField(max_length=100, blank=True, null=True, help_text="Colour of the icon")
    title = models.CharField(max_length=100,  blank=True, null=True)
    label = models.CharField(max_length=100,  blank=True, null=True)
    type = models.CharField(max_length=100, choices=action_types.choices, default=action_types.SINGLE, blank=True, null=True )
    error_message = models.CharField(max_length=100, blank=True, null=True)
    icon_class = models.CharField(max_length=100, help_text="Font Awesome icon class", blank=True, null=True)
    action = models.CharField(max_length=100, help_text="Name of javascript function to be performed on click", blank=True, null=True)

    def __str__(self):
        return self.name + " : " + self.title + " : " + self.type

    def create_record_action_button(self, name, title, label, type, error_message, icon_class, action, icon_colour):
        self.name = name
        self.title = title
        self.label = label
        self.type = type
        self.error_message = error_message
        self.icon_class = icon_class
        self.action = action
        self.icon_colour = icon_colour
        self.save()
        return self

    def remove_all_record_action_buttons(self):
        RecordActionButton.objects.all().delete()
        return True

    def get_record_action_button(self):
        return RecordActionButton.objects.all()

''' 
class SidebarPanel(models.Model):
    name = models.CharField(max_length=100, unique=True)
    description = models.CharField(max_length=100)
    icon = models.CharField(max_length=100)
    action = models.CharField(max_length=100)
    action_type = models.CharField(max_length=100)
    action_url = models.CharField(max_length=100)
    action_data = models.CharField(max_length=100)
    action_target = models.CharField(max_length=100)
    action_icon = models.CharField(max_length=100)
    action_icon_class = models.CharField(max_length=100)
    action_colour = models.CharField(max_length=100)
    action_class = models.CharField(max_length=100)
    action_text = models.CharField(max_length=100)
    action_tooltip = models.CharField(max_length=100)
    action_tooltip_class = models.CharField(max_length=100)

    def __str__(self):
        return self.name + " : " + self.description

    def create_sidebar_panel(self, name, description, icon, action, action_type, action_url, action_data, action_target, action_icon, action_icon_class, action_colour, action_class, action_text, action_tooltip, action_tooltip_class):
        self.name = name
        self.description = description
        self.icon = icon
        self.action = action
        self.action_type = action_type
        self.action_url = action_url
        self.action_data = action_data
        self.action_target = action_target
        self.action_icon = action_icon
        self.action_icon_class = action_icon_class
        self.action_colour = action_colour
        self.action_class = action_class
        self.action_text = action_text
        self.action_tooltip = action_tooltip
        self.action_tooltip_class = action_tooltip_class
        self.save()
        return self

    def remove_all_sidebar_panels(self):
        SidebarPanel.objects.all().delete()
        return True

    def get_sidebar_panels(self):
        return SidebarPanel.objects.all()
'''
class Component(models.Model):
    class Meta:
        ordering = ['title']

    title_buttons = models.ManyToManyField(TitleButton, blank=True)
    recordaction_buttons = models.ManyToManyField(RecordActionButton, blank=True)
    name = models.CharField(max_length=100, unique=True)
    title = models.CharField(max_length=100)
    subtitle = models.CharField(max_length=100, blank=True, null=True)
    widget_icon = models.CharField(max_length=100, blank=True, null=True)
    widget_colour = models.CharField(max_length=200,blank=True, null=True)
    widget_icon_class = models.CharField(max_length=100, blank=True, null=True)
    table_id = models.CharField(max_length=100)
    reverse_url = models.CharField(max_length=100,blank=True, null=True)


    def __str__(self):
        return self.name + " : " + self.title

    def create_component(self, name, title, subtitle, widget_icon, widget_colour, widget_icon_class, table_id, reverse_url):
        self.name = name
        self.title = title
        self.subtitle = subtitle
        self.widget_icon = widget_icon
        self.widget_colour = widget_colour
        self.widget_icon_class = widget_icon_class
        self.table_id = table_id
        self.reverse_url = reverse_url
        self.save()
        return self
    
    def remove_all_components(self):
        Component.objects.all().delete()
        return True

    def get_components(self):
        return Component.objects.all()
    

#db.Profiles.updateMany({"associated_type.value":"BGE"},{"$set":{"associated_type.$":"BGE"}})
class ProfileType(models.Model):
    class Meta:
        ordering = ['description']
        
    associated_profile_types = models.ManyToManyField(AssociatedProfileType, blank=True)
    components = models.ManyToManyField(Component, blank=True)
    type = models.CharField(max_length=20, unique=True)
    description = models.CharField(max_length=100)
    widget_colour = models.CharField(max_length=200, blank=True, null=True)
    is_dtol_profile = models.BooleanField(default=False)
    is_permission_required = models.BooleanField(default=True)
    post_save_action = models.CharField(max_length=100, blank=True, null=True)
    pre_save_action = models.CharField(max_length=100, blank=True, null=True)

    def __str__(self):
        return self.type + " : " + self.description

    def create_profile_type(self, type, description, widget_colour, is_dtol_profile, is_permission_required, post_save_action=None, pre_save_action=None):
        self.type = type
        self.description = description
        self.widget_colour = widget_colour
        self.is_dtol_profile = is_dtol_profile
        self.is_permission_required = is_permission_required
        self.post_save_action = post_save_action
        self.pre_save_action = pre_save_action
        self.save()
        return self

    def remove_all_profile_types(self):
        ProfileType.objects.all().delete()
        return True
    
    def get_profile_types(self):
        return ProfileType.objects.all()
            

@receiver(post_save, sender=SequencingCentre)
@receiver(post_save, sender=AssociatedProfileType)
@receiver(post_save, sender=ProfileType)
def refresh_schema(sender, instance, **kwargs):
    DataSchemas.refresh() 