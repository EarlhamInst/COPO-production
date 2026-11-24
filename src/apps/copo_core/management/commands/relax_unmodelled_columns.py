'''Make required columns that this branch's models don't define optional.

Switching from a branch whose migrations added a required column (e.g. `edp`'s
`profiletype.is_sample_released_on_submission`) to one that doesn't have that
field leaves the column in the local database. `migrate` can't remove it, since
the migration that added it doesn't exist on this branch. Django then leaves it
out of every INSERT, which fails with "null value in column ... violates
not-null constraint" (for example, `setup_profile_types` on every profile type).

This command finds such columns, i.e. NOT NULL, without a database default and
not a field of the table's model, and drops their NOT NULL constraint so that
this branch can insert rows again. The branch that added the column always
sets it, so switching back is unaffected.

It only runs with DEBUG=True, since it is meant for local databases that are
shared between branches. The post-checkout hook in deployment/git-hooks runs it
after `migrate`.
'''

from django.apps import apps
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connection

UNMODELLED_REQUIRED_COLUMNS_SQL = '''
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = current_schema()
      AND table_name = %s
      AND is_nullable = 'NO'
      AND column_default IS NULL
'''


def get_tables_and_modelled_columns():
    # Return {table: {columns of its model's fields}} for this branch's models.
    # Proxy and unmanaged models are skipped: a proxy shares its concrete model's
    # table but has no fields of its own, so every column would look unmodelled.
    tables = {}
    for model in apps.get_models(include_auto_created=True):
        meta = model._meta
        if meta.proxy or not meta.managed:
            continue
        columns = tables.setdefault(meta.db_table, set())
        columns.update(field.column for field in meta.local_concrete_fields)
    return tables


class Command(BaseCommand):
    help = ("Drop NOT NULL from columns that other branches' migrations added but this "
            "branch's models don't define (local databases only)")

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true',
                            help='List the columns without changing them')

    def handle(self, *args, **options):
        if not settings.DEBUG:
            raise CommandError('relax_unmodelled_columns only runs with DEBUG=True (local databases)')
        if connection.vendor != 'postgresql':
            self.stdout.write(f'Skipped: only PostgreSQL is supported, not {connection.vendor}')
            return

        quote = connection.ops.quote_name
        relaxed = []
        with connection.cursor() as cursor:
            existing_tables = set(connection.introspection.table_names(cursor))
            for table, modelled_columns in sorted(get_tables_and_modelled_columns().items()):
                if table not in existing_tables:
                    continue
                cursor.execute(UNMODELLED_REQUIRED_COLUMNS_SQL, [table])
                for (column,) in cursor.fetchall():
                    if column in modelled_columns:
                        continue
                    if not options['dry_run']:
                        cursor.execute(f'ALTER TABLE {quote(table)} ALTER COLUMN {quote(column)} DROP NOT NULL')
                    relaxed.append(f'{table}.{column}')

        if not relaxed:
            self.stdout.write('No required columns outside this branch\'s models')
            return
        action = 'Would make optional' if options['dry_run'] else 'Made optional'
        for column in relaxed:
            self.stdout.write(f'{action}: {column}')
