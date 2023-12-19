from bson import ObjectId
from django.core.management import BaseCommand

from common.dal import copo_base_da as da


# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    # Show this when the user types help
    help = "strip white space from dtol species list entries in given manifest id"

    def __init__(self):
        super().__init__()

    def add_arguments(self, parser):
        parser.add_argument('manifest_id', type=str)

    # A command must define handle()
    def handle(self, *args, **options):
        manifest_id = options["manifest_id"]
        fromdb = da.handle_dict["sample"].find({"manifest_id": manifest_id})
        fromdb = da.cursor_to_list(fromdb)
        for s in fromdb:
            for d in s["species_list"]:
                for el in d:
                    d[el] = str(d[el]).strip()
                    print(d[el])
            da.handle_dict["sample"].update_one(
                {"_id": ObjectId(s["_id"])},
                {"$set": {"species_list": s["species_list"]}}
            )
        print("done")
