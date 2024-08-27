from typing import Any
from django.core.management.base import BaseCommand
from src.apps.copo_core.models import AssociatedProfileType
from django.contrib.auth.models import User


# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    # Show this when the user types help
    help = "Add associated profile types to the database, which will be used to show the relevant profiles on the Accept/Reject web page"

    def __init__(self):
        super().__init__()

    def handle(self, *args, **options):
        self.stdout.write("Removing Existing Associated Profile Types")
        AssociatedProfileType().remove_all_associated_profile_types()
        self.stdout.write("Adding Associated Profile Types")

        AssociatedProfileType().create_associated_profile_type(name="ASG",                                                        
                                                              label="Aquatic Symbiosis Genomics (ASG)")

        AssociatedProfileType().create_associated_profile_type(name="BGE",
                                                               label="Biodiversity Genomics Europe (BGE)")
        
        AssociatedProfileType().create_associated_profile_type(name="CBP",
                                                               label="Catalan Initiative for the Earth BioGenome Project (CBP)",
                                                               is_approval_required=True)
        
        AssociatedProfileType().create_associated_profile_type(name="DTOL",
                                                              label="Darwin Tree of Life (DTOL)")
        
        AssociatedProfileType().create_associated_profile_type(name="DTOL_ENV",
                                                              label="Darwin Tree of Life Environmental Samples (DTOL_ENV)")
        
        AssociatedProfileType().create_associated_profile_type(name="ERGA",
                                                             label="European Reference Genome Atlas (ERGA)")
        
        AssociatedProfileType().create_associated_profile_type(name="ERGA_PILOT",
                                                             label="European Reference Genome Atlas - Pilot (ERGA_PILOT)")
        
        AssociatedProfileType().create_associated_profile_type(name="ERGA_COMMUNITY",
                                                             label="European Reference Genome Atlas - COMMUNITY (ERGA_COMMUNITY)")
        
        AssociatedProfileType().create_associated_profile_type(name="POP_GENOMICS",
                                                             label="Population Genomics (POP_GENOMICS)")                                 

        self.stdout.write("Associated Profile Types Added")
        records = AssociatedProfileType.objects.all()

        for record in records:
            self.stdout.write(record.name)
