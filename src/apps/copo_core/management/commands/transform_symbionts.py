from django.core.management import BaseCommand
from common.dal.sample_da import Sample
from bson import ObjectId
import pprint

pp = pprint.PrettyPrinter(indent=2)


# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):

    # A command must define handle()
    def handle(self, *args, **options):
        s_list = Sample().get_collection_handle().find({"tol_project": "ASG"})
        to_remove = list()
        for s in s_list:
            print("processing..." + s["SPECIMEN_ID"])

            for idx, el in enumerate(s["species_list"]):
                if el["SYMBIONT"] == "symbiont":
                    symbiont = s.copy()
                    symbiont["species_list"] = list()
                    symbiont["species_list"].append(el)
                    to_remove.append({
                        'RACK_OR_PLATE_ID': el["RACK_OR_PLATE_ID"], 'TUBE_OR_WELL_ID': el["TUBE_OR_WELL_ID"],
                        "id": s["_id"]})

                    symbiont["LIFESTAGE"] = "NOT_COLLECTED"
                    symbiont["SEX"] = "NOT_COLLECTED"
                    symbiont["ORGANISM_PART"] = "WHOLE_ORGANISM"

                    symbiont.pop("_id")
                    Sample().get_collection_handle().insert_one(symbiont)

        for remove in to_remove:
            Sample().get_collection_handle().update_one({"_id": ObjectId(remove["id"])}, {"$pull": {"species_list": {
                'RACK_OR_PLATE_ID': remove["RACK_OR_PLATE_ID"], 'TUBE_OR_WELL_ID': remove["TUBE_OR_WELL_ID"]}}})
