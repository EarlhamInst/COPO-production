from typing import Any
from django.core.management.base import BaseCommand
from src.apps.copo_core.models import ProfileType, Component, RecordActionButton, TitleButton, AssociatedProfileType

'''
ProfileType
 id |   type   |                     description                     | widget_colour | is_dtol_profile | is_permission_required 
----+----------+-----------------------------------------------------+---------------+-----------------+------------------------
  5 | erga     | European Reference Genome Atlas (ERGA)              | #E61A8D       | t               | t
  4 | asg      | Aquatic Symbiosis Genomics (ASG)                    | #5829bb       | t               | t
  3 | dtolenv  | Darwin Tree of Life Environmental Samples (DTOLENV) | #fb7d0d       | t               | t
  2 | dtol     | Darwin Tree of Life (DTOL)                          | #16ab39       | t               | t
  1 | genomics | Stand-alone                                         | #009c95       | f               | f
  6 | test     | Test New Profile                                    | violet        | f               | t

'''

"""
Component
 id |         name         |        title         | widget_icon  | widget_colour | widget_icon_class  |      table_id       |                    reverse_url                     |      subtitle       
----+----------------------+----------------------+--------------+---------------+--------------------+---------------------+----------------------------------------------------+---------------------
  3 | accessions_dashboard | Accessions           | pink         |               | fa fa-sitemap      | accessions_table    | copo_accession:copo_accessions                     | 
  4 | assembly             | Assembly             | puzzle piece | violet        | fa fa-puzzle-piece | assembly_table      | copo_assembly_submission:copo_assembly             | 
  5 | taggedseq            | Barcoding Manifests  | barcode      | red           | fa fa-barcode      | tagged_seq_table    | copo_barcoding_submission:copo_taggedseq           | #component_subtitle
  6 | files                | Files                | file         | blue          | fa fa-file         | files_table         | copo_file:copo_files                               | 
  9 | seqannotation        | Sequence Annotations | tag          | yellow        | fa fa-tag          | seqannotation_table | copo_seq_annotation_submission:copo_seq_annotation | 
  8 | read                 | Reads                | dna          | orange        | fa fa-dna          | read_table          | copo_read_submission:copo_reads                    | #component_subtitle
  7 | sample               | Samples              | filter       | olive         | fa fa-filter       | sample_table        | copo_sample:copo_samples                           | 
  2 | accessions           | Accessions           | sitemap      | pink          | fa fa-sitemap      | accessions_table    | copo_accession:copo_accessions                     | 
  1 | profile              | Work Profiles        |              |               |                    | copo_profiles_table |                                                    | #component_subtitle

"""
"""
RecordActionButton

 id |              name               |                   title                    |          label           |  type  |                                     error_message                                     |      icon_class       |          action          | icon_colour 
----+---------------------------------+--------------------------------------------+--------------------------+--------+---------------------------------------------------------------------------------------+-----------------------+--------------------------+-------------
  8 | add_terminal_all                | Add new file by terminal                   | Add                      |        |                                                                                       | fa fa-terminal        | add_files_by_terminal    | blue
 10 | download_sample_manifest_single | Download Sample Manifest                   | Download sample manifest | single | Please select one of samples in the manifest to download                              | fa fa-download        | download-sample-manifest | blue
  7 | add_local_all                   | Add new file by browsing local file system | Add                      |        | Add new file by browsing local file system                                            | fa fa-desktop         | add_files_locally        | blue
  2 | edit_record_single              | Edit record                                | Edit                     | single | Please select a record to edit                                                        | fa fa-pencil-square-o | edit                     | green
  1 | add_record_all                  | Add new record                             | Add                      |        |                                                                                       | fa fa-plus            | add                      | blue
 12 | download_permits_multiple       | Download Permits                           | Download permits         | multi  | Please select one or more sample records from the table shown to download permits for | fa fa-download        | download-permits         | orange
 11 | view_images_multiple            | View Images                                | View images              | multi  | Please select one or more sample records from the table shown to view images for      | fa fa-eye             | view-images              | teal
  9 | submit_tagged_seq_multi         | Submit Tagged Sequence                     | Submit                   | multi  | Please select one or more record to submit                                            | fa fa-info            | submit_tagged_seq        | teal
  6 | submit_read_multi               | Submit Read                                | Submit                   | multi  | Please select one or more record to submit                                            | fa fa-info            | submit_read              | teal
  5 | submit_annotation_multi         | Submit Annotation                          | Submit                   | multi  | Please select one or more record to submit                                            | fa fa-info            | submit_annotation        | teal
  4 | submit_assembly_multi           | Submit Assembly                            | Submit                   | multi  | Please select one or more record to submit                                            | fa fa-info            | submit_assembly          | 
  3 | delete_record_multi             | Delete records                             | Delete                   | multi  | Please select one or more records to delete                                           | fa fa-trash-can       | validate_and_delete      | red
 13 | releasestudy                    | Release Study                              | Release Study            | single |                                                                                       | fa fa-globe           | release_study            | blue
"""
"""
TitleButton
 id |                name                |                                                                                                                        template                                                                                                                         |     additional_attr      
----+------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------
 10 | accept_reject_samples              | <button style="display: none" title="Accept/Reject TOL Samples"             class="big circular ui icon teal button accept_reject_samples copo-tooltip">         <i class="icon tasks sign"></i>     </button>                                          | 
 11 | tol_inspect                        | <button style="display: none" title="Inspect TOL"             class="big circular ui icon yellow button tol_inspect copo-tooltip">         <i class="icon clipboard list"></i>     </button>                                                            | 
 12 | tol_inspect_gal                    | <button class="big circular ui icon green button tol_inspect_gal copo-tooltip" title="Inspect TOL by GAL">         <i class="icon building"></i>     </button>                                                                                          | 
 13 | copo_accessions                    | <button style="display: none" title="View Accessions Dashboard"             class="big circular ui icon pink button copo_accessions copo-tooltip">         <i class="icon sitemap"></i>     </button>                                                   | 
  7 | new_taggedseq_spreadsheet_template | <button style="display: inline" title="Add Tagged Sequence (s) from Tagged Sequence Spreadsheet"             class="big circular ui icon button new-taggedseq-spreadsheet-template copo-tooltip">         <i class="icon table sign"></i>     </button> | 
  6 | new_terminal_file                  | <button title="Add new file by terminal"             class="big circular ui icon primary button new-terminal-file copo-tooltip">         <i class="icon terminal sign"></i>     </button>                                                               | 
  5 | new_local_file                     | <button title="Add new file by browsing local file system"             class="big circular ui icon primary button new-local-file copo-tooltip">         <i class="icon desktop sign"></i>     </button>                                                 | 
  4 | new_reads_spreadsheet_template     | <button style="display: inline" title="Add Read(s) from Read Spreadsheet"             class="big circular ui icon button new-reads-spreadsheet-template copo-tooltip">         <i class="icon table sign"></i>     </button>                            | 
  3 | new_samples_spreadsheet_template   | <button   title="Add Sample(s) from Spreadsheet"             class="big circular ui icon button new-samples-spreadsheet-template copo-tooltip">         <i class="icon table sign"></i>     </button>                                                   | 
  2 | quick_tour_template                | <button title="Quick tour"             class="big circular ui icon orange button takeatour quick-tour-template copo-tooltip">         <i class="icon lightbulb"></i>     </button>                                                                      | 
  1 | new_component_template             | <button title="Add new profile record"             class="big circular ui icon primary button new-component-template copo-tooltip">         <i class="icon add sign"></i>     </button>                                                                 | 
  9 | download_sop                       | <a title="Download Standard Operating Procedure (SOP)"         class="big circular ui icon yellow button download-sop copo-tooltip" target="_blank">         <i class="icon download sign"></i>     </a>                                                | href:#sop_url
  8 | download_blank_manifest_template   | <a  title="Download Blank Manifest Template"             class="big circular ui icon brown button download-blank-manifest-template copo-tooltip" target="_blank">         <i class="icon download sign"></i>     </a>                                   | href:#blank_manifest_url
"""

# The class must be named Command, and subclass BaseCommand
class Command(BaseCommand):
    # Show this when the user types help
    help = "Add profile type definition to the database "

    def __init__(self):
        super().__init__()

    def handle(self, *args, **options):
        

        self.stdout.write("Removing Record Action Button ")
        RecordActionButton().remove_all_record_action_buttons()
        self.stdout.write("Adding Record Action Button ")

        add_terminal_all = RecordActionButton().create_record_action_button(name="add_terminal_all", title="Add new file by terminal", label="Add", type="", error_message="", icon_class="fa fa-terminal", action="add_files_by_terminal", icon_colour="blue")
        download_sample_manifest_single = RecordActionButton().create_record_action_button(name="download_sample_manifest_single", title="Download Sample Manifest", label="Download sample manifest", type="single", error_message="Please select one of samples in the manifest to download", icon_class="fa fa-download", action="download-sample-manifest", icon_colour="blue")
        add_local_all = RecordActionButton().create_record_action_button(name="add_local_all", title="Add new file by browsing local file system", label="Add", type="", error_message="Add new file by browsing local file system", icon_class="fa fa-desktop", action="add_files_locally", icon_colour="blue")
        edit_record_single = RecordActionButton().create_record_action_button(name="edit_record_single", title="Edit record", label="Edit", type="single", error_message="Please select a record to edit", icon_class="fa fa-pencil-square", action="edit", icon_colour="green")
        add_record_all = RecordActionButton().create_record_action_button(name="add_record_all", title="Add new record", label="Add", type="", error_message="", icon_class="fa fa-plus-circle", action="add", icon_colour="blue")
        download_permits_multiple = RecordActionButton().create_record_action_button(name="download_permits_multiple", title="Download Permits", label="Download permits", type="multi", error_message="Please select one or more sample records from the table shown to download permits for", icon_class="fa fa-download", action="download-permits", icon_colour="orange")
        view_images_multiple = RecordActionButton().create_record_action_button(name="view_images_multiple", title="View Images", label="View images", type="multi", error_message="Please select one or more sample records from the table shown to view images for", icon_class="fa fa-eye", action="view-images", icon_colour="teal")
        submit_tagged_seq_multi = RecordActionButton().create_record_action_button(name="submit_tagged_seq_multi", title="Submit Tagged Sequence", label="Submit", type="multi", error_message="Please select one or more record to submit", icon_class="fa fa-info-circle", action="submit_tagged_seq", icon_colour="teal")
        submit_read_multi = RecordActionButton().create_record_action_button(name="submit_read_multi", title="Submit Read", label="Submit", type="multi", error_message="Please select one or more record to submit", icon_class="fa fa-info-circle", action="submit_read", icon_colour="teal")
        submit_annotation_multi = RecordActionButton().create_record_action_button(name="submit_annotation_multi", title="Submit Annotation", label="Submit", type="multi", error_message="Please select one or more record to submit", icon_class="fa fa-info-circle", action="submit_annotation", icon_colour="teal")
        submit_assembly_multi = RecordActionButton().create_record_action_button(name="submit_assembly_multi", title="Submit Assembly", label="Submit", type="multi", error_message="Please select one or more record to submit", icon_class="fa fa-info-circle", action="submit_assembly", icon_colour="teal")
        delete_record_multi = RecordActionButton().create_record_action_button(name="delete_record_multi", title="Delete records", label="Delete", type="multi", error_message="Please select one or more records to delete", icon_class="fa fa-trash-can", action="validate_and_delete", icon_colour="red")
        releasestudy = RecordActionButton().create_record_action_button(name="releasestudy", title="Release Study", label="Release Study", type="single", error_message="", icon_class="fa fa-globe", action="release_study", icon_colour="blue")
        self.stdout.write("Record Action Button Added")
        records = RecordActionButton.objects.all()

        for record in records:
            self.stdout.write(record.name)

        self.stdout.write("Removing Title Button ")
        TitleButton().remove_all_title_buttons()
        self.stdout.write("Adding Title Button ")

        accept_reject_samples = TitleButton().create_title_button(name="accept_reject_samples", template="<button style=\"display: none\" title=\"Accept/Reject TOL Samples\"             class=\"big circular ui icon teal button accept_reject_samples copo-tooltip\">         <i class=\"icon tasks sign\"></i>     </button>", additional_attr="") 
        tol_inspect = TitleButton().create_title_button(name="tol_inspect", template="<button style=\"display: none\" title=\"Inspect TOL\"             class=\"big circular ui icon yellow button tol_inspect copo-tooltip\">         <i class=\"icon clipboard list\"></i>     </button>", additional_attr="")
        tol_inspect_gal = TitleButton().create_title_button(name="tol_inspect_gal", template="<button class=\"big circular ui icon green button tol_inspect_gal copo-tooltip\" title=\"Inspect TOL by GAL\">         <i class=\"icon building\"></i>     </button>", additional_attr="")
        copo_accessions = TitleButton().create_title_button(name="copo_accessions", template="<button style=\"display: none\" title=\"View Accessions Dashboard\"             class=\"big circular ui icon pink button copo_accessions copo-tooltip\">         <i class=\"icon sitemap\"></i>     </button>", additional_attr="")
        new_taggedseq_spreadsheet_template = TitleButton().create_title_button(name="new_taggedseq_spreadsheet_template", template="<button style=\"display: inline\" title=\"Add Tagged Sequence (s) from Tagged Sequence Spreadsheet\"             class=\"big circular ui icon button new-taggedseq-spreadsheet-template copo-tooltip\">         <i class=\"icon table sign\"></i>     </button>", additional_attr="")
        new_terminal_file = TitleButton().create_title_button(name="new_terminal_file", template="<button title=\"Add new file by terminal\"             class=\"big circular ui icon primary button new-terminal-file copo-tooltip\">         <i class=\"icon terminal sign\"></i>     </button>", additional_attr="")
        new_local_file = TitleButton().create_title_button(name="new_local_file", template="<button title=\"Add new file by browsing local file system\"             class=\"big circular ui icon primary button new-local-file copo-tooltip\">         <i class=\"icon desktop sign\"></i>     </button>", additional_attr="")
        new_reads_spreadsheet_template = TitleButton().create_title_button(name="new_reads_spreadsheet_template", template="<button style=\"display: inline\" title=\"Add Read(s) from Read Spreadsheet\"             class=\"big circular ui icon button new-reads-spreadsheet-template copo-tooltip\">         <i class=\"icon table sign\"></i>     </button>", additional_attr="")
        new_samples_spreadsheet_template = TitleButton().create_title_button(name="new_samples_spreadsheet_template", template="<button   title=\"Add Sample(s) from Spreadsheet\"             class=\"big circular ui icon button new-samples-spreadsheet-template copo-tooltip\">         <i class=\"icon table sign\"></i>     </button>", additional_attr="")
        quick_tour_template = TitleButton().create_title_button(name="quick_tour_template", template="<button title=\"Quick tour\"             class=\"big circular ui icon orange button takeatour quick-tour-template copo-tooltip\">         <i class=\"icon lightbulb\"></i>     </button>", additional_attr="")
        new_component_template = TitleButton().create_title_button(name="new_component_template", template="<button title=\"Add new profile record\"             class=\"big circular ui icon primary button new-component-template copo-tooltip\">         <i class=\"icon add sign\"></i>     </button>", additional_attr="")
        download_sop = TitleButton().create_title_button(name="download_sop", template="<a title=\"Download Standard Operating Procedure (SOP)\"         class=\"big circular ui icon yellow button download-sop copo-tooltip\" target=\"_blank\">         <i class=\"icon download sign\"></i>     </a>", additional_attr="href:#sop_url")
        download_blank_manifest_template = TitleButton().create_title_button(name="download_blank_manifest_template", template="<a  title=\"Download Blank Manifest Template\"             class=\"big circular ui icon brown button download-blank-manifest-template copo-tooltip\" target=\"_blank\">         <i class=\"icon download sign\"></i>     </a>", additional_attr="href:#blank_manifest_url")
 
        self.stdout.write("Title Button Added")
        records = TitleButton.objects.all()

        for record in records:
            self.stdout.write(record.name)

        self.stdout.write("Setup Completed")


        self.stdout.write("Removing Component ")
        Component().remove_all_components()
        self.stdout.write("Adding Component ")

        assembly = Component().create_component(name="assembly", title="Assembly", widget_icon="puzzle piece", widget_colour="violet", widget_icon_class="fa fa-puzzle-piece", table_id="assembly_table", reverse_url="copo_assembly_submission:copo_assembly", subtitle="")
        taggedseq = Component().create_component(name="taggedseq", title="Barcoding Manifests", widget_icon="barcode", widget_colour="red", widget_icon_class="fa fa-barcode", table_id="tagged_seq_table", reverse_url="copo_barcoding_submission:copo_taggedseq", subtitle="#component_subtitle")
        files = Component().create_component(name="files", title="Files", widget_icon="file", widget_colour="blue", widget_icon_class="fa fa-file", table_id="files_table", reverse_url="copo_file:copo_files", subtitle="")
        seqannotation = Component().create_component(name="seqannotation", title="Sequence Annotations", widget_icon="tag", widget_colour="yellow", widget_icon_class="fa fa-tag", table_id="seqannotation_table", reverse_url="copo_seq_annotation_submission:copo_seq_annotation", subtitle="")
        read = Component().create_component(name="read", title="Reads", widget_icon="dna", widget_colour="orange", widget_icon_class="fa fa-dna", table_id="read_table", reverse_url="copo_read_submission:copo_reads", subtitle="#component_subtitle")
        sample = Component().create_component(name="sample", title="Samples", widget_icon="filter", widget_colour="olive", widget_icon_class="fa fa-filter", table_id="sample_table", reverse_url="copo_sample:copo_samples", subtitle="")
        accessions = Component().create_component(name="accessions", title="Accessions", widget_icon="sitemap", widget_colour="pink", widget_icon_class="fa fa-sitemap", table_id="accessions_table", reverse_url="copo_accession:copo_accessions", subtitle="")
        profile = Component().create_component(name="profile", title="Work Profiles", widget_icon="", widget_colour="", widget_icon_class="", table_id="copo_profiles_table", reverse_url="", subtitle="#component_subtitle")

        assembly.recordaction_buttons.set([add_record_all, edit_record_single, delete_record_multi, submit_assembly_multi])
        assembly.title_buttons.set([new_component_template])

        taggedseq.recordaction_buttons.set([add_record_all, edit_record_single, delete_record_multi, submit_tagged_seq_multi])
        taggedseq.title_buttons.set([new_taggedseq_spreadsheet_template, download_blank_manifest_template])

        files.recordaction_buttons.set([add_local_all, add_terminal_all, delete_record_multi])
        files.title_buttons.set([new_local_file, new_terminal_file])

        seqannotation.recordaction_buttons.set([add_record_all, edit_record_single, delete_record_multi, submit_annotation_multi])  
        seqannotation.title_buttons.set([new_component_template])

        read.recordaction_buttons.set([delete_record_multi, submit_read_multi])
        read.title_buttons.set([new_reads_spreadsheet_template, download_blank_manifest_template])

        sample.recordaction_buttons.set([download_sample_manifest_single, download_permits_multiple, view_images_multiple])
        sample.title_buttons.set([quick_tour_template, new_samples_spreadsheet_template, download_blank_manifest_template, download_sop, accept_reject_samples])

        accessions.title_buttons.set([copo_accessions, accept_reject_samples, tol_inspect, tol_inspect_gal])

        profile.recordaction_buttons.set([releasestudy])
        profile.title_buttons.set([quick_tour_template, new_component_template])

        self.stdout.write("Component Added")
        records = Component.objects.all()
        
        for record in records:
            self.stdout.write(record.name)
        
        self.stdout.write("Removing Existing Profile Types ")
        ProfileType().remove_all_profile_types()
        self.stdout.write("Adding Profile Types")

        erga = ProfileType().create_profile_type(type="erga",description="European Reference Genome Atlas (ERGA)", widget_colour="#E61A8D", is_dtol_profile=True, is_permission_required=True, post_save_action="src.apps.copo_profile.utils.profile_utils.post_save_dtol_profile", pre_save_action="src.apps.copo_profile.utils.profile_utils.pre_save_erga_profile")
        asg = ProfileType().create_profile_type(type="asg",description="Aquatic Symbiosis Genomics (ASG)", widget_colour="#5829bb", is_dtol_profile=True, is_permission_required=True, post_save_action="src.apps.copo_profile.utils.profile_utils.post_save_dtol_profile")
        dtolenv = ProfileType().create_profile_type(type="dtolenv",description="Darwin Tree of Life Environmental Samples (DTOLENV)", widget_colour="#fb7d0d", is_dtol_profile=True, is_permission_required=True)
        dtol = ProfileType().create_profile_type(type="dtol",description="Darwin Tree of Life (DTOL)", widget_colour="#16ab39", is_dtol_profile=True, is_permission_required=True, post_save_action="src.apps.copo_profile.utils.profile_utils.post_save_dtol_profile")
        genomics = ProfileType().create_profile_type(type="genomics",description="Genomics", widget_colour="#009c95", is_dtol_profile=False, is_permission_required=False)

        erga.components.set([assembly, taggedseq, files, seqannotation, read, sample, accessions])
        asg.components.set([assembly, taggedseq, files, seqannotation, read, sample, accessions])
        dtolenv.components.set([assembly, taggedseq, files, seqannotation, read, sample, accessions])
        dtol.components.set([assembly, taggedseq, files, seqannotation, read, sample, accessions])
        genomics.components.set([assembly, files, seqannotation, read, sample, accessions])


        at_asg = AssociatedProfileType.objects.get(name="ASG")
        at_bge = AssociatedProfileType.objects.get(name="BGE")
        at_bioblitz = AssociatedProfileType.objects.get(name="BIOBLITZ")  
        at_cbp = AssociatedProfileType.objects.get(name="CBP") 
        at_dtol = AssociatedProfileType.objects.get(name="DTOL")
        at_dtolenv = AssociatedProfileType.objects.get(name="DTOL_ENV")
        at_erga = AssociatedProfileType.objects.get(name="ERGA") 
        at_erga_pilot = AssociatedProfileType.objects.get(name="ERGA_PILOT")
        at_erga_community = AssociatedProfileType.objects.get(name="ERGA_COMMUNITY")
        at_pop_genomics = AssociatedProfileType.objects.get(name="POP_GENOMICS")                              
        at_sanger = AssociatedProfileType.objects.get(name="SANGER")  


        erga.associated_profile_types.set([at_erga, at_bge, at_bioblitz, at_cbp, at_erga_pilot, at_erga_community, at_pop_genomics, at_sanger])
        asg.associated_profile_types.set([at_asg])
        dtolenv.associated_profile_types.set([at_dtolenv])
        dtol.associated_profile_types.set([at_dtol])
        
        self.stdout.write("Profile Types Added")
        records = ProfileType.objects.all()

        for record in records:
            self.stdout.write(record.type)