from django.core.management.base import BaseCommand

import psycopg
from common.utils import helpers


class Command(BaseCommand):
    help = 'Generate social accounts'

    def handle(self, *args, **options):
        db_host = helpers.get_env('POSTGRES_SERVICE')
        db_name = helpers.get_env('POSTGRES_DB')
        db_user = helpers.get_env('POSTGRES_USER')
        db_pass = helpers.get_env('POSTGRES_PASSWORD')
        db_port = helpers.get_env('POSTGRES_PORT')

        conn_string = "dbname=%s user=%s password=%s host=%s port=%s" % (
            db_name,
            db_user,
            db_pass,
            db_host,
            db_port,
        )

        try:
            print("Connecting to database...")
            conn = psycopg.connect(conn_string)

            cursor = conn.cursor()
            print("Connected!\n")
        except (Exception, psycopg.DatabaseError) as error:
            self.stdout.write(
                self.style.ERROR(
                    "Encountered error while creating social accounts: " + str(error)
                )
            )
            raise

        # clear target tables preparatory for new data
        try:
            print("Deleting from target tables...\n")

            cursor.execute("DELETE FROM socialaccount_socialapp_sites")
            cursor.execute("DELETE FROM socialaccount_socialapp")
            cursor.execute("DELETE FROM django_site")

        except (Exception, psycopg.DatabaseError) as error:
            self.stdout.write(
                self.style.ERROR(
                    "Encountered error while creating social accounts: " + str(error)
                )
            )
            raise

        try:
            cursor.execute(
                "INSERT INTO django_site (id, domain, name) VALUES (%s, %s, %s)",
                (1, "www.copo-project.org", "www.copo-project.org"),
            )

            ORCID_SECRET = helpers.get_env('ORCID_SECRET')
            ORCID_CLEINT = helpers.get_env('ORCID_CLIENT')

            cursor.execute(
                "INSERT INTO socialaccount_socialapp (id, provider_id, provider, name, client_id, secret, key, settings) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                (1, "", "orcid", "Orcid", ORCID_CLEINT, ORCID_SECRET, " ", "{}"),
            )

            print("Creating 'socialaccount_socialapp_sites' record...\n")
            cursor.execute(
                "INSERT INTO socialaccount_socialapp_sites (id, socialapp_id, site_id) VALUES (%s, %s, %s)",
                (1, 1, 1),
            )

            self.stdout.write(
                self.style.SUCCESS('Successfully created social accounts')
            )
        except (Exception, psycopg.DatabaseError) as error:
            self.stdout.write(
                self.style.ERROR(
                    "Encountered error while creating social accounts: " + str(error)
                )
            )
            raise

        # commit and close connection
        conn.commit()
        cursor.close()
        conn.close()
