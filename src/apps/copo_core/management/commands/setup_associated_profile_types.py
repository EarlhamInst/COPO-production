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
                                                               label="Biodiversity Genomics Europe (BGE)",
                                                               is_approval_required=True,
                                                               is_acceptance_email_notification_required=True,
                                                               acceptance_email_body="Dear {gretting},<br><br>We have now accepted your manifest submission of <b>{title}</b> in Collaborative OPen Omics (COPO).<br><ul><li>In case you have not provided barcoding, biobanking and voucher information yet, with the submitted manifest, please provide it as soon as available, by uploading a new manifest updating solely the respective fields.<br>This information is required to be displayed with the Sample data in ENA and will be part of the Genome note.</li></li><br><li>If you have not done it yet, please complete the two Material Transfer Agreement (MTAs) documents. After completing your part of the documents, please send them to Thomas Marcussen [<a href='mailto:thomarc@ibv.uio.no'>thomarc@ibv.uio.no</a>] and Rita Monteiro [<a href='mailto:r.monteiro@leibniz-lib.de'>r.monteiro@leibniz-lib.de</a>].<ul><br><li>MTA1 - Material Transfer Agreement for PROVISION OF MATERIAL, with no change in ownership (between Sample Provider and Sequencing Centre): <a href='https://eu.twk.pm/jsmsel4ird'>https://eu.twk.pm/jsmsel4ird</a></li><br><li>MTA2 - Material Transfer Agreement for RECEIPT OF MATERIAL, with change in ownership (between Sample Provider and LIB Biobank): <a href='https://eu.twk.pm/h3nyix8dk9'>https://eu.twk.pm/h3nyix8dk9</a></li></ul></li><br><li>Please proceed to sample shipping with {centre_labels}. The contact person is in cc.</li><br><li>Please <b>do not</b> ship the samples before the MTAs have been signed or without prior agreement with the Sequencing Centre.</li></ul><br><br>If you have any questions, please do not hesitate to get in touch.<br><br><br>Best regards,<br>Collaborative OPen Omics (COPO) Project Team"),
        
        AssociatedProfileType().create_associated_profile_type(name="BIOBLITZ",
                                                                label="BioBlitz",
                                                                is_approval_required=True,
                                                                is_acceptance_email_notification_required=True,
                                                                acceptance_email_body="Dear {gretting},<br><br>We have now accepted your manifest submission of <b>{title}</b> in Collaborative OPen Omics (COPO).<br><br><br>If you have any questions, please do not hesitate to get in touch.<br><br><br>Best regards,<br>Collaborative OPen Omics (COPO) Project Team")
        
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
        
        AssociatedProfileType().create_associated_profile_type(name="ERGA_SATELLITES",
                                                             label="European Reference Genome Atlas - Satellites (ERGA_SATELLITES)")
        
        AssociatedProfileType().create_associated_profile_type(name="POP_GENOMICS",
                                                             label="Population Genomics (POP_GENOMICS)",
                                                             is_approval_required=True)                                 

        self.stdout.write("Associated Profile Types Added")
        records = AssociatedProfileType.objects.all()

        for record in records:
            self.stdout.write(record.name)
