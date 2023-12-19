from django.core.management import BaseCommand
from common.dal.sample_da import Sample


# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    help = "update old DTOL samples in db to move taxonomy metadata intospecies_list"

    def __init__(self):
        self.TAXONOMY_FIELDS = ["TAXON_ID", "ORDER_OR_GROUP", "FAMILY", "GENUS",
                                "SCIENTIFIC_NAME", "INFRASPECIFIC_EPITHET", "CULTURE_OR_STRAIN_ID",
                                "COMMON_NAME", "TAXON_REMARKS"]

    def handle(self, *args, **options):
        samples_to_update = self.identify_old_dtol_samples()
        for sample in samples_to_update:
            self.move_taxonomy_information(sample)

    def identify_old_dtol_samples(self):
        '''identify DTOL samples in the old format
        look for sample_type dtol and no species_list'''
        listtoupdate = []
        for dtolsample in Sample().get_all_dtol_samples():
            if "species_list" not in Sample().get_record(dtolsample['_id']):
                listtoupdate.append(dtolsample['_id'])
        return listtoupdate

    def move_taxonomy_information(self, oid):
        '''create species_list field in database and
        add all taxonomic field in the first item of the list
        then remove the same field from the root'''
        print(oid)
        sam = Sample().get_record(oid)
        out = dict()
        out["SYMBIONT"] = "target"
        for field in self.TAXONOMY_FIELDS:
            out[field] = sam[field]
        topass = []
        topass.append(out)
        Sample().update_field("species_list", topass, oid)
        # now remove field from outside species_list
        for field in self.TAXONOMY_FIELDS:
            Sample().remove_field(field, oid)
        return
