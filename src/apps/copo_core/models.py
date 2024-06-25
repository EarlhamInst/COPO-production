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
from django.contrib.auth.models import Group


class UserDetails(models.Model):
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


class banner_view(models.Model):
    header_txt = models.TextField(max_length=100, blank=False, default="")
    body_txt = models.TextField(max_length=2000, blank=False, default="")
    active = models.BooleanField()


class ViewLock(models.Model):
    url = models.URLField(max_length=250)
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

    def __str__(self):
        return self.name

    def create_associated_profile_type(self, name, label, is_approval_required=False ):
        self.name = name
        self.label = label
        self.is_approval_required = is_approval_required
        self.save()
        return self

    def remove_all_associated_profile_types(self):
        AssociatedProfileType.objects.all().delete()
        return True

    def get_associated_profile_types(self):
        return AssociatedProfileType.objects.all()