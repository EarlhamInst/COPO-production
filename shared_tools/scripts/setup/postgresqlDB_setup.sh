#!/bin/bash

# Run the following in the terminal of COPO root project folder

# Create PostgreSQL database, user and permissions
POSTGRES_USER=copo_user
POSTGRES_DB=copo
POSTGRES_PASSWORD=password

sudo -u postgres createuser -s $POSTGRES_USER
sudo -u postgres createdb $POSTGRES_DB
sudo -u postgres psql postgres -c "alter user $POSTGRES_USER with encrypted password '$POSTGRES_PASSWORD';"
sudo -u postgres psql postgres -c "grant all privileges on database $POSTGRES_DB to $POSTGRES_USER ;"
sudo -u postgres psql postgres -c "ALTER USER $POSTGRES_USER CREATEDB;"

# Run Django/COPO setup functions
python manage.py makemigrations
python manage.py makemigrations allauth
python manage.py migrate
python manage.py setup_groups
python manage.py setup_schemas
python manage.py createcachetable
python manage.py social_accounts
python manage.py setup_sequencing_centres
python manage.py setup_associated_profile_types
python manage.py setup_profile_types
python manage.py setup_news
python manage.py createsuperuser # Used to create an admin for the Django admin interface

# Setup allauth social accounts...
# N.B. To set up ORCID, you should have environmental variables set for ORCID_CLIENT_ID and ORCID_SECRET
# Replace <enter-ORCID-client-ID> with the ORCID_CLIENT_ID
# Replace <enter-ORCID-secret> with the ORCID_SECRET
export PGPASSWORD=$POSTGRES_PASSWORD; psql -h 'localhost' -U  $POSTGRES_USER -d 'copo' -c 'DELETE FROM socialaccount_socialapp_sites'
export PGPASSWORD=$POSTGRES_PASSWORD; psql -h 'localhost' -U  $POSTGRES_USER -d 'copo' -c 'DELETE FROM django_site'
export PGPASSWORD=$POSTGRES_PASSWORD; psql -h 'localhost' -U  $POSTGRES_USER -d 'copo' -c 'DELETE FROM socialaccount_socialapp'
export PGPASSWORD=$POSTGRES_PASSWORD; psql -h 'localhost' -U  $POSTGRES_USER -d 'copo' -c "INSERT INTO django_site (id, domain, name) VALUES (1, 'www.copo-project.org', 'www.copo-project.org')"
export PGPASSWORD=$POSTGRES_PASSWORD; psql -h 'localhost' -U  $POSTGRES_USER -d 'copo' -c 'INSERT INTO socialaccount_socialapp_sites (id, socialapp_id, site_id) VALUES (1, 1, 1)'
export PGPASSWORD=$POSTGRES_PASSWORD; psql -h 'localhost' -U  $POSTGRES_USER -d 'copo' -c "INSERT INTO socialaccount_socialapp (id, provider, name, client_id, secret, key, provider_id, settings) VALUES (1, 'orcid', 'Orcid', '<enter-ORCID-client-ID>', '<enter-ORCID-secret>', '' , '', '{}'::json)"