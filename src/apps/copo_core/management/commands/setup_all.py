# Runs the full local COPO bootstrap sequence in one go.
# Mirrors the "Run Django/COPO setup functions" block of
# shared_tools/scripts/setup/postgresqlDB_setup.sh, but as a single Django
# management command so it can be launched from VS Code ("Python: Setup All").

from django.contrib.auth import get_user_model
from django.core.management import BaseCommand, call_command


# Commands that must succeed for the bootstrap to be meaningful. If one of these
# fails the whole run aborts; any other (seed) step logs its error and continues,
# so re-running on an already-seeded DB (e.g. superuser already exists) is safe.
CRITICAL = {"makemigrations", "migrate"}

# Ordered bootstrap steps: (human-readable label, command name, positional args).
# Order matters — migrations must be applied before any seed command runs.
STEPS = [
    ("Make migrations", "makemigrations", []),
    ("Make allauth migrations", "makemigrations", ["allauth"]),
    ("Apply migrations", "migrate", []),
    ("Set up groups", "setup_groups", []),
    ("Set up schemas", "setup_schemas", []),
    ("Create cache table", "createcachetable", []),
    ("Configure social accounts", "social_accounts", []),
    ("Set up sequencing centres", "setup_sequencing_centres", []),
    ("Set up associated profile types", "setup_associated_profile_types", []),
    ("Set up profile types", "setup_profile_types", []),
    ("Set up news", "setup_news", []),
    ("Create superuser", "createsuperuser", []),  # interactive — prompts on stdin
]


# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    help = "Run the full local COPO bootstrap (migrations + all setup_* seed commands) in one go."

    def handle(self, *args, **options):
        total = len(STEPS)
        for index, (label, command_name, command_args) in enumerate(STEPS, start=1):
            self.stdout.write(self.style.MIGRATE_HEADING(
                f"\n[{index}/{total}] {label}  (manage.py {command_name} {' '.join(command_args)})".rstrip()
            ))
            if command_name == "createsuperuser" and self._superuser_exists():
                self.stdout.write(self.style.WARNING("⚠ Create superuser skipped: a superuser already exists"))
                continue
            self._run_step(label, command_name, command_args)

    @staticmethod
    def _superuser_exists():
        return get_user_model().objects.filter(is_superuser=True).exists()

    def _run_step(self, label, command_name, command_args):
        try:
            call_command(command_name, *command_args)
        except Exception as error:
            if command_name in CRITICAL:
                self.stderr.write(self.style.ERROR(f"✗ {label} failed (critical): {error}"))
                raise
            self.stderr.write(self.style.WARNING(f"⚠ {label} skipped: {error}"))
            return
        self.stdout.write(self.style.SUCCESS(f"✓ {label}"))
