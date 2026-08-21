# Create a local user for browser tests, so they can log in via
# /accounts/login/ instead of driving real ORCID OAuth.
#
# COPO's login page only offers the ORCID button, but allauth's own LoginView is
# mounted at /accounts/login/ and ModelBackend is enabled, so a username and
# password work without any change to application code. This command just makes
# sure a suitable user exists in the target deployment.
#
# Refuses to run against production. Intended for the test/demo stacks only:
#   python manage.py create_test_user --password "$COPO_TEST_USER_PASSWORD"

from django.conf import settings
from django.contrib.auth.models import User, Group 
from django.core.management import BaseCommand, CommandError

DEFAULT_USERNAME = 'copo_browser_test_user'
DEFAULT_EMAIL = 'browser-test-user@example.invalid'

# Environments this command is allowed to touch. Production is never in this
# list: a known-password user is a liability anywhere real data lives.
ALLOWED_ENVIRONMENTS = {'test', 'local', 'demo', 'dev'}


class Command(BaseCommand):
    help = 'Create or update a user for browser tests to log in as'

    def add_arguments(self, parser):
        parser.add_argument('--username', default=DEFAULT_USERNAME)
        parser.add_argument('--email', default=DEFAULT_EMAIL)
        parser.add_argument(
            '--password',
            required=True,
            help='Read this from an env var; do not hardcode it in a compose file',
        )

    def handle(self, *args, **options):
        environment = getattr(settings, 'ENVIRONMENT_TYPE', '')
        if environment not in ALLOWED_ENVIRONMENTS:
            raise CommandError(
                f'Refusing to create a test user in ENVIRONMENT_TYPE={environment!r}. '
                f'Allowed: {sorted(ALLOWED_ENVIRONMENTS)}'
            )
        password = options['password']
        # check password in not empty
        if not password.strip():
            raise CommandError(
                '--password was empty. If you passed "$COPO_TEST_USER_PASSWORD", '
                'that environment variable is not set in this shell or container.'
            )
        # check password length
        if not len(password) >= 8:
            raise CommandError(
                '--password must be at least 8 characters long. '
                'This is a safety check to avoid creating a weak password in a test/demo stack.'
            )
        user = self.build_user(
            username=options['username'],
            email=options['email'],
            password=options['password'],
        )

        self.stdout.write(
            f'Test user {user.username!r} ready '
            f'(groups: {sorted(g.name for g in user.groups.all()) or "none"})'
        )

    def build_user(self, username, email, password):
        """Create or update the test user and return it.

        Creating a User is enough to get UserDetails and an API token, which the
        post_save receivers in copo_core.models add automatically.
        """
        
        user, created = User.objects.get_or_create(username=username, defaults={'email': email})
        self.stdout.write(f'{"Creating" if created else "Updating"} test user {username!r}...')
        user.email = email
        # set_password() re-hashes unconditionally, even for an unchanged
        # password — Django ties session validity to a hash of the password
        # (get_session_auth_hash()), so a no-op password "change" silently
        # invalidates every existing session for this user, including any
        # cached storage_state from earlier in the same test run. Only
        # reset it when the password has actually changed.
        if not user.check_password(password):
            user.set_password(password)
        
        groups = Group.objects.all()
        if not groups:
            raise CommandError(
                'No groups exist in the database. Run `python manage.py setup_groups` first.'
            )
        for group in groups:
            user.groups.add(group)
        user.save()
        return user
