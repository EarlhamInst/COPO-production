from typing import Any
from django.core.management.base import BaseCommand
from src.apps.copo_core.models import SequencingCentre
from common.dal.copo_base_da import DataSchemas


# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    # Show this when the user types help
    help = "Add sequencing centres to the database, which will be used to direct notifications to the correct email addresses"

    def __init__(self):
        super().__init__()

    def handle(self, *args, **options):
        self.stdout.write("Removing Existing Sequencing Centres")
        SequencingCentre().remove_all_sequencing_centres()
        self.stdout.write("Adding Sequencing Centres")

        SequencingCentre().create_sequencing_centre(name="UNIBA",
                                                             description="UNIVERSITY OF BARI",
                                                             label="UNIVERSITY OF BARI",
                                                             contact_details='[{"contact_name": "Carmela Gissi", "contact_email": "carmela.gissi@uniba.it"},{"contact_name": "Claudio Ciofi", "contact_email": "claudio.ciofi@unifi.it"}]')

        SequencingCentre().create_sequencing_centre(name="BIDGEN",
                                     description="DNA SEQUENCING AND GENOMICS LABORATORY, HELSINKI GENOMICS CORE FACILITY",
                                     label="DNA SEQUENCING AND GENOMICS LABORATORY, HELSINKI GENOMICS CORE FACILITY")
        
        SequencingCentre().create_sequencing_centre(name="CNAG",
                                     description="CENTRO NACIONAL DE ANÁLISIS GENÓMICO ",
                                     label="CENTRO NACIONAL DE ANÁLISIS GENÓMICO",
                                     contact_details='[{"contact_name": "Sanchez Escudero", "contact_email": "ignacio.sanchez@cnag.eu"},{"contact_name": "Maria Aguilera", "contact_email": "laura.aguilera@cnag.eu"}]')
        
        SequencingCentre().create_sequencing_centre(name="EI",
                                      description="EARLHAM INSTITUTE",
                                      label="EARLHAM INSTITUTE")
        
        SequencingCentre().create_sequencing_centre(name="FGCZ",
                                                             description="FUNCTIONAL GENOMIC CENTER ZURICH",
                                                             label="FUNCTIONAL GENOMIC CENTER ZURICH")
        
        SequencingCentre().create_sequencing_centre(name="GENOSCOPE",
                                                             description="GENOSCOPE",
                                                             label="GENOSCOPE",
                                                             contact_details='[{"contact_name":"Pedro Oliveira", "contact_email":"pcoutool@genoscope.cns.fr"}]')
        
        SequencingCentre().create_sequencing_centre(name="GTF",
                                                             description="LAUSANNE GENOMIC TECHNOLOGIES FACILITY",
                                                             label="LAUSANNE GENOMIC TECHNOLOGIES FACILITY")
        
        SequencingCentre().create_sequencing_centre(name="HANSEN",
                                                             description="HANSEN LAB, DENMARK",
                                                             label="HANSEN LAB, DENMARK")
        
        SequencingCentre().create_sequencing_centre(name="LIB",
                                                             description="LEIBNIZ INSTITUTE FOR THE ANALYSIS OF BIODIVERSITY CHANGE, MUSEUM KOENIG, BONN",
                                                             label="LEIBNIZ INSTITUTE FOR THE ANALYSIS OF BIODIVERSITY CHANGE, MUSEUM KOENIG, BONN")        

        SequencingCentre().create_sequencing_centre(name="NCCT",
                                                             description="NGS COMPETENCE CENTER TÜBINGEN",
                                                             label="NGS COMPETENCE CENTER TÜBINGEN")
        
        SequencingCentre().create_sequencing_centre(name="NGS",
                                                             description="NGS BERN",
                                                             label="NGS BERN")
        
        SequencingCentre().create_sequencing_centre(name="NSC",
                                                             description="NORWEGIAN SEQUENCING CENTRE",
                                                             label="NORWEGIAN SEQUENCING CENTRE",
                                                             contact_details='[{"contact_name":"Ave Tooming-Klunderud", "contact_email":"ave.tooming-klunderud@ibv.uio.no"}]')  
        
        SequencingCentre().create_sequencing_centre(name="NSF",
                                                             description="NEUROMICS SUPPORT FACILITY, UANTWERP, VIB",
                                                             label="NEUROMICS SUPPORT FACILITY, UANTWERP, VIB")
        
        SequencingCentre().create_sequencing_centre(name="PARTNER",
                                                             description="INDUSTRY PARTNER",
                                                             label="INDUSTRY PARTNER")
                           
        SequencingCentre().create_sequencing_centre(name="SANGER",
                                                             description="SANGER INSTITUTE",
                                                             label="SANGER INSTITUTE",
                                                             contact_details='[{"contact_name":"Wiesia Johnson", "contact_email":"wj3@sanger.ac.uk"}]',
                                                             is_approval_required=True)


        SequencingCentre().create_sequencing_centre(name="SCILIFE",
                                                             description="SCILIFELAB",
                                                             label="SCILIFELAB",
                                                             contact_details='[{"contact_name":"Olga Vinnere Pettersson", "contact_email":"olga.pettersson@scilifelab.uu.se"}]')
        
        SequencingCentre().create_sequencing_centre(name="SVARDAL",
                                                             description="SVARDAL LAB, ANTWERP",
                                                             label="SVARDAL LAB, ANTWERP")
        
        SequencingCentre().create_sequencing_centre(name="UNIFI",
                                                             description="UNIVERSITY OF FLORENCE",
                                                             label="UNIVERSITY OF FLORENCE",
                                                             contact_details='[{"contact_name":"Claudio Ciofi", "contact_email":"claudio.ciofi@unifi.it"}]')

        SequencingCentre().create_sequencing_centre(name="WGGC",
                                                             description="WEST GERMAN GENOME CENTRE",
                                                             label="WEST GERMAN GENOME CENTRE")
                       
        SequencingCentre().create_sequencing_centre(name="OTHER",
                                                            description="Other associated sequencing centre",
                                                            label="OTHER")                                     

        self.stdout.write("Sequencing Centres Added")
        records = SequencingCentre.objects.all()

        for record in records:
            self.stdout.write(record.name)

        #refresh the schema in case it changes the schema
        DataSchemas.refresh()