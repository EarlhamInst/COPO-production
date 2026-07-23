# Delete MongoDB profiles and every record associated with them.
#
# "Associated" = any document, in ANY collection, whose `profile_id` field
# matches a target profile's id (associated records store profile_id as the
# STRING form of the profile's ObjectId). Collections are enumerated at runtime
# so newly-added record types are covered automatically.
#
# SAFETY: dry-run by default — it only reports. Deleting requires BOTH --commit
# and a typed confirmation (or --yes to skip the prompt in automation).
#
# NOTE: this is a raw Mongo wipe. It does NOT run the per-profile post_delete
# hooks (e.g. the EDP Sapio project sync) that Profile.validate_and_delete would
# — it clears local Mongo state only.

from django.core.management import BaseCommand

from common.dal.mongo_util import get_collection_ref

PROFILE_COLLECTION = "Profiles"


class Command(BaseCommand):
    help = ("Delete MongoDB profiles and all records associated with them (by profile_id). "
            "Dry-run unless --commit is given.")

    def add_arguments(self, parser):
        parser.add_argument(
            "--type", dest="profile_type", default=None,
            help="Only clear profiles of this type (e.g. ei_edp). Default: ALL profiles.")
        parser.add_argument(
            "--commit", action="store_true",
            help="Actually delete. Without this the command only reports what it would do.")
        parser.add_argument(
            "--yes", action="store_true",
            help="Skip the interactive confirmation (use with --commit in scripts).")
        parser.add_argument(
            "--include-orphans", action="store_true",
            help="Also delete records whose profile_id points at a profile that will not "
                 "survive (i.e. orphaned records left by earlier deletions). Only meaningful "
                 "with no --type filter, or alongside it to also sweep danglers.")

    def handle(self, *args, **options):
        profiles = get_collection_ref(PROFILE_COLLECTION)
        db = profiles.database

        query = {"type": options["profile_type"]} if options["profile_type"] else {}
        profile_docs = list(profiles.find(query, {"_id": 1, "title": 1, "type": 1}))
        if not profile_docs:
            self.stdout.write(self.style.WARNING("No matching profiles found — nothing to do."))
            return

        profile_object_ids = [p["_id"] for p in profile_docs]
        profile_ids = [str(_id) for _id in profile_object_ids]

        # The predicate for "an associated record to delete". By default that's records
        # tied to the profiles being removed. With --include-orphans it's any record whose
        # profile_id is NOT a surviving profile (covers both associated and dangling records).
        if options["include_orphans"]:
            surviving_ids = [str(p["_id"]) for p in profiles.find(
                {"_id": {"$nin": profile_object_ids}}, {"_id": 1})]
            record_filter = {"profile_id": {"$exists": True, "$nin": surviving_ids}}
        else:
            record_filter = {"profile_id": {"$in": profile_ids}}

        # Count associated records across EVERY collection (keyed by profile_id string).
        assoc = {}
        for name in sorted(db.list_collection_names()):
            if name == PROFILE_COLLECTION:
                continue
            n = db[name].count_documents(record_filter)
            if n:
                assoc[name] = n
        total_assoc = sum(assoc.values())

        # Report.
        scope = f"type={options['profile_type']}" if options["profile_type"] else "ALL types"
        self.stdout.write(self.style.MIGRATE_HEADING(
            f"\nProfiles to delete: {len(profile_ids)}  ({scope})"))
        for p in profile_docs:
            self.stdout.write(f"  - {p.get('title', '(no title)')}  [{p.get('type', '?')}]  {p['_id']}")
        self.stdout.write(self.style.MIGRATE_HEADING("Associated records to delete:"))
        for name, n in assoc.items():
            self.stdout.write(f"  {name:32} {n}")
        if not assoc:
            self.stdout.write("  (none)")
        self.stdout.write(
            f"\nTOTAL: {len(profile_ids)} profiles + {total_assoc} associated records "
            f"across {len(assoc)} collections")

        if not options["commit"]:
            self.stdout.write(self.style.WARNING(
                "\nDRY RUN — nothing deleted. Re-run with --commit to execute."))
            return

        if not options["yes"]:
            answer = input(
                f"\nType 'delete' to PERMANENTLY remove these {len(profile_ids)} profiles "
                f"and {total_assoc} associated records: ")
            if answer.strip().lower() != "delete":
                self.stdout.write(self.style.ERROR("Aborted — no changes made."))
                return

        # Delete associated records first, then the profiles themselves.
        deleted_assoc = 0
        for name in assoc:
            result = db[name].delete_many(record_filter)
            deleted_assoc += result.deleted_count
            self.stdout.write(self.style.SUCCESS(f"  deleted {result.deleted_count} from {name}"))
        result = profiles.delete_many({"_id": {"$in": profile_object_ids}})
        self.stdout.write(self.style.SUCCESS(
            f"\nDone: deleted {result.deleted_count} profiles + {deleted_assoc} associated records."))
