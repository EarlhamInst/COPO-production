from bson import ObjectId
from django.contrib.auth.models import User
from django.core.management.base import BaseCommand

from common.dal.profile_da import Profile
from common.dal.submission_da import Submission


class Command(BaseCommand):
    help = 'Generate Submission Report'

    def handle(self, *args, **options):

        subs = Submission().get_all_records(sort_by="date_created")

        for s in subs:

            if "project" in s.get("accessions", {}):
                try:
                    acc = s["accessions"]["project"][0]["accession"]
                except IndexError as e:
                    acc = "No Accession"
                except KeyError as e:
                    acc = "No Accession"
            elif "study_accessions" in s.get("accessions", {}):
                acc = s["accessions"]["study_accessions"]["bioProjectAccession"]
            else:
                acc = "No Accession"
            date = s.get("date_created", "No Date")
            profile = Profile().get_record(ObjectId(s.get("profile_id", "")))
            if profile:
                title = profile.get("title", "No Title")
                user = User.objects.get(pk=profile.get("user_id", ""))
                print(acc + ", " + title + ", " + user.first_name + " " + user.last_name + ", " + str(date))
            else:
                title = "No Title"
                print(acc + ", " + title + ", " + "No Names,  " + str(date))
