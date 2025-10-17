from typing import Any
from django.core.management.base import BaseCommand
from src.apps.copo_core.models import (
    ProfileType,
    Component,
    RecordActionButton,
    TitleButton,
    AssociatedProfileType,
)
from common.dal.copo_base_da import DataSchemas

'''
*** ProfileType ***

 id |   type   |                     description                     | widget_colour | is_dtol_profile | is_permission_required 
----+----------+-----------------------------------------------------+---------------+-----------------+------------------------
  1 | asg      | Aquatic Symbiosis Genomics (ASG)                    | #5829bb       | t               | t
  2 | dtol     | Darwin Tree of Life (DTOL)                          | #16ab39       | t               | t
  3 | dtolenv  | Darwin Tree of Life Environmental Samples (DTOLENV) | #fb7d0d       | t               | t
  4 | erga     | European Reference Genome Atlas (ERGA)              | #E61A8D       | t               | t
  5 | genomics | Genomics                                            | #009c95       | f               | f
  6 | test     | Test New Profile                                    | violet        | f               | t

'''

"""
*** Component ***

 id |         name         |        title         | widget_icon  | widget_colour | widget_icon_class  |      table_id       |                    reverse_url                     |      subtitle       
----+----------------------+----------------------+--------------+---------------+--------------------+---------------------+----------------------------------------------------+---------------------
  1 | accessions           | Accessions           | sitemap      | pink          | fa fa-sitemap      | accessions_table    | copo_accession:copo_accessions                     | 
  2 | accessions_dashboard | Accessions           | pink         |               | fa fa-sitemap      | accessions_table    | copo_accession:copo_accessions                     | 
  3 | assembly             | Assembly             | puzzle piece | violet        | fa fa-puzzle-piece | assembly_table      | copo_assembly_submission:copo_assembly             | 
  4 | files                | Files                | file         | blue          | fa fa-file         | files_table         | copo_file:copo_files                               | 
  5 | general_sample       | Samples              | filter       | olive         | fa fa-filter       | sample_table        | copo_sample:copo_general_samples                   | #component_subtitle
  6 | images_rembi         | REMBI                | image        | coral-pink    | fa fa-image        | images_table        | copo_single_cell_submission:copo_singlecell        | #component_subtitle
  7 | images_stx_fish      | ST FISH              | image        | terra-cotta   | fa fa-image        | images_table        | copo_single_cell_submission:copo_singlecell        | #component_subtitle
  8 | profile              | Work Profiles        |              |               |                    | copo_profiles_table |                                                    | #component_subtitle
  9 | read                 | Reads                | dna          | orange        | fa fa-dna          | read_table          | copo_read_submission:copo_reads                    | #component_subtitle
 10 | sample               | Samples              | filter       | olive         | fa fa-filter       | sample_table        | copo_sample:copo_samples                           | 
 11 | seqannotation        | Sequence Annotations | tag          | yellow        | fa fa-tag          | seqannotation_table | copo_seq_annotation_submission:copo_seq_annotation | 
 12 | singlecell           | Single-cell          | dna          | green         | fa fa-dna          | singlecell_table    | copo_single_cell_submission:copo_singlecell        | #component_subtitle
 13 | taggedseq            | Barcoding Manifests  | barcode      | red           | fa fa-barcode      | tagged_seq_table    | copo_barcoding_submission:copo_taggedseq           | #component_subtitle
"""

"""
** RecordActionButton ***

 id |              name                       |                   title                    |          label           |  type  |                                     error_message                                     |      icon_class       |          action          | icon_colour 
----+-----------------------------------------+--------------------------------------------+--------------------------+--------+---------------------------------------------------------------------------------------+-----------------------+--------------------------+-------------
  1 | add_local_all                           | Add new file by browsing local file system | Add                      |        | Add new file by browsing local file system                                            | fa fa-desktop         | add_files_locally        | blue
  2 | add_record_all                          | Add new record                             | Add                      |        |                                                                                       | fa fa-plus            | add                      | blue
  3 | add_terminal_all                        | Add new file by terminal                   | Add                      |        |                                                                                       | fa fa-terminal        | add_files_by_terminal    | blue
  4 | delete_images_multi                     | Delete records                             | Delete                   | multi  | Please select one or more records to delete                                           | fa fa-trash-can       | delete_images            | red
  5 | delete_read_multi                       | Delete records                             | Delete                   | multi  | Please select one or more records to delete                                           | fa fa-trash-can       | delete_read              | red
  6 | delete_record_multi                     | Delete records                             | Delete                   | multi  | Please select one or more records to delete                                           | fa fa-trash-can       | validate_and_delete      | red
  7 | delete_singlecell_multi                 | Delete records                             | Delete                   | multi  | Please select one or more records to delete                                           | fa fa-trash-can       | delete_singlecell        | red
  8 | download_general_sample_manifest_single | Download Sample Manifest                   | Download sample manifest | single | Please select one of samples in the manifest to download                              | fa fa-download        | download-sample-manifest | blue
  9 | download_permits_multiple               | Download Permits                           | Download permits         | multi  | Please select one or more sample records from the table shown to download permits for | fa fa-download        | download-permits         | orange
 10 | download_sample_manifest_single         | Download Sample Manifest                   | Download sample manifest | single | Please select one of samples in the manifest to download                              | fa fa-download        | download-sample-manifest | blue
 11 | edit_record_single                      | Edit record                                | Edit                     | single | Please select a record to edit                                                        | fa fa-pencil-square-o | edit                     | green
 12 | publish_singlecell_single_ena           | Publish Single-cell Records to ENA         | Publish to ENA           | single | Please select one record to publish                                                   | fa fa-info-circle     | publish_singlecell_ena   | teal
 13 | publish_singlecell_single_zenodo        | Publish Single-cell Records to ZENODO      | Publish to ZENODO        | single | Please select one record to publish                                                   | fa fa-info-circle     | publish_singlecell_zenodo| blue
 14 | releasestudy                            | Release Study                              | Release Study            | single |                                                                                       | fa fa-globe           | release_study            | blue
 15 | submit_annotation_multi                 | Submit Annotation                          | Submit                   | multi  | Please select one or more record to submit                                            | fa fa-info            | submit_annotation        | teal
 16 | submit_assembly_multi                   | Submit Assembly                            | Submit                   | multi  | Please select one or more record to submit                                            | fa fa-info            | submit_assembly          | teal
 17 | submit_read_multi                       | Submit Read                                | Submit                   | multi  | Please select one or more record to submit                                            | fa fa-info            | submit_read              | teal
 18 | submit_singlecell_single_ena            | Submit Single-cell Records to ENA          | Submit to ENA            | single | Please select one record to submit                                                    | fa fa-info-circle     | submit_singlecell_ena    | teal
 19 | submit_singlecell_single_zenodo         | Submit Single-cell Records to ZENODO       | Submit to ZENODO         | single | Please select one record to submit                                                    | fa fa-info-circle     | submit_singlecell_zenodo | blue
 20 | submit_tagged_seq_multi                 | Submit Tagged Sequence                     | Submit                   | multi  | Please select one or more record to submit                                            | fa fa-info            | submit_tagged_seq        | teal
 21 | view_images_multiple                    | View Images                                | View images              | multi  | Please select one or more sample records from the table shown to view images for      | fa fa-eye             | view-images              | teal  
"""

"""
*** TitleButton ***

 id |                name                     |                                                                                                                        template                                                                                                                                                                                 |     additional_attr      
----+-----------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------
  1 | accept_reject_samples                   | <button style="display: none" title="Accept/Reject TOL Samples"             class="big circular ui icon teal button accept_reject_samples copo-tooltip">         <i class="icon tasks sign"></i>     </button>                                                                                                  | 
  2 | copo_accessions                         | <button style="display: none" title="View Accessions Dashboard"             class="big circular ui icon pink button copo_accessions copo-tooltip">         <i class="icon sitemap"></i>     </button>                                                                                                           | 
  3 | download_blank_manifest_template        | <a  title="Download Blank Manifest Template"             class="big circular ui icon brown button download-blank-manifest-template copo-tooltip" target="_blank">         <i class="icon download sign"></i>     </a>                                                                                           | href:#blank_manifest_url
  4 | download_general_sample_manifest_single | <button style="display: inline" title="Download Sample Manifest" label="Download sample manifest" type="single" error_message="Please select one of samples in the manifest to download" class="big circular ui icon button download-sample-manifest copo-tooltip"> <i class="icon download sign"></i></button> | 
  5 | download_sop                            | <a title="Download Standard Operating Procedure (SOP)"         class="big circular ui icon yellow button download-sop copo-tooltip" target="_blank">         <i class="icon download sign"></i>     </a>                                                                                                        | href:#sop_url
  6 | new_component_template                  | <button title="Add new profile record"             class="big circular ui icon primary button new-component-template copo-tooltip">         <i class="icon add sign"></i>     </button>                                                                                                                         | 
  7 | new_image_spreadsheet_template          | <button style="display: inline" title="Add study from Image Spreadsheet"  class="big circular ui icon button new-singlecell-spreadsheet-template copo-tooltip"> <i class="icon table sign"></i></button>                                                                                                        | 
  8 | new_local_file                          | <button title="Add new file by browsing local file system"             class="big circular ui icon primary button new-local-file copo-tooltip">         <i class="icon desktop sign"></i>     </button>                                                                                                         | 
  9 | new_reads_spreadsheet_template          | <button style="display: inline" title="Add Read(s) from Read Spreadsheet"             class="big circular ui icon button new-reads-spreadsheet-template copo-tooltip">         <i class="icon table sign"></i>     </button>                                                                                    | 
 10 | new_samples_spreadsheet_template        | <button   title="Add/Update sample(s) from spreadsheet"             class="big circular ui icon button new-samples-spreadsheet-template copo-tooltip">         <i class="icon table sign"></i>     </button>                                                                                                    | 
 11 | new_singlecell_spreadsheet_template     | <button style="display: inline" title="Add study from Single-cell Spreadsheet" class="big circular ui icon button new-singlecell-spreadsheet-template copo-tooltip"> <i class="icon table sign"></i></button>                                                                                                   | 
 12 | new_taggedseq_spreadsheet_template      | <button style="display: inline" title="Add Tagged Sequence (s) from Tagged Sequence Spreadsheet"             class="big circular ui icon button new-taggedseq-spreadsheet-template copo-tooltip">         <i class="icon table sign"></i>     </button>                                                         | 
 13 | new_terminal_file                       | <button title="Add new file by terminal"             class="big circular ui icon primary button new-terminal-file copo-tooltip">         <i class="icon terminal sign"></i>     </button>                                                                                                                       | 
 14 | quick_tour_template                     | <button title="Quick tour"             class="big circular ui icon orange button takeatour quick-tour-template copo-tooltip">         <i class="icon lightbulb"></i>     </button>                                                                                                                              | 
 15 | tol_inspect                             | <button style="display: none" title="Inspect TOL"             class="big circular ui icon yellow button tol_inspect copo-tooltip">         <i class="icon clipboard list"></i>     </button>                                                                                                                    | 
 16 | tol_inspect_gal                         | <button class="big circular ui icon green button tol_inspect_gal copo-tooltip" title="Inspect TOL by GAL">         <i class="icon building"></i>     </button>                                                                                                                                                  | 
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

        add_terminal_all = RecordActionButton().create_record_action_button(
            name="add_terminal_all",
            title="Add new file by terminal",
            label="Add",
            type="",
            error_message="",
            icon_class="fa fa-terminal",
            action="add_files_by_terminal",
            icon_colour="blue",
        )
        download_sample_manifest_single = RecordActionButton().create_record_action_button(
            name="download_sample_manifest_single",
            title="Download Sample Manifest",
            label="Download sample manifest",
            type="single",
            error_message="Please select one of samples in the manifest to download",
            icon_class="fa fa-download",
            action="download-sample-manifest",
            icon_colour="blue",
        )
        download_singlecell_manifest_single = RecordActionButton().create_record_action_button(
            name="download_singlecell_manifest_single",
            title="Download manifest",
            label="Download manifest",
            type="single",
            error_message="Please select one of studies in the manifest to download",
            icon_class="fa fa-download",
            action="download-singlecell-manifest",
            icon_colour="blue",
        )
        add_local_all = RecordActionButton().create_record_action_button(
            name="add_local_all",
            title="Add new file by browsing local file system",
            label="Add",
            type="",
            error_message="Add new file by browsing local file system",
            icon_class="fa fa-desktop",
            action="add_files_locally",
            icon_colour="blue",
        )
        edit_record_single = RecordActionButton().create_record_action_button(
            name="edit_record_single",
            title="Edit record",
            label="Edit",
            type="single",
            error_message="Please select a record to edit",
            icon_class="fa fa-pencil-square",
            action="edit",
            icon_colour="green",
        )
        add_record_all = RecordActionButton().create_record_action_button(
            name="add_record_all",
            title="Add new record",
            label="Add",
            type="",
            error_message="",
            icon_class="fa fa-plus-circle",
            action="add",
            icon_colour="blue",
        )
        download_permits_multiple = RecordActionButton().create_record_action_button(
            name="download_permits_multiple",
            title="Download Permits",
            label="Download permits",
            type="multi",
            error_message="Please select one or more sample records from the table shown to download permits for",
            icon_class="fa fa-download",
            action="download-permits",
            icon_colour="orange",
        )
        view_images_multiple = RecordActionButton().create_record_action_button(
            name="view_images_multiple",
            title="View Images",
            label="View images",
            type="multi",
            error_message="Please select one or more sample records from the table shown to view images for",
            icon_class="fa fa-eye",
            action="view-images",
            icon_colour="teal",
        )
        submit_tagged_seq_multi = RecordActionButton().create_record_action_button(
            name="submit_tagged_seq_multi",
            title="Submit Tagged Sequence",
            label="Submit",
            type="multi",
            error_message="Please select one or more record to submit",
            icon_class="fa fa-info-circle",
            action="submit_tagged_seq",
            icon_colour="teal",
        )
        submit_read_multi = RecordActionButton().create_record_action_button(
            name="submit_read_multi",
            title="Submit Read",
            label="Submit",
            type="multi",
            error_message="Please select one or more record to submit",
            icon_class="fa fa-info-circle",
            action="submit_read",
            icon_colour="teal",
        )
        submit_annotation_multi = RecordActionButton().create_record_action_button(
            name="submit_annotation_multi",
            title="Submit Annotation",
            label="Submit",
            type="multi",
            error_message="Please select one or more record to submit",
            icon_class="fa fa-info-circle",
            action="submit_annotation",
            icon_colour="teal",
        )
        submit_assembly_multi = RecordActionButton().create_record_action_button(
            name="submit_assembly_multi",
            title="Submit Assembly",
            label="Submit",
            type="multi",
            error_message="Please select one or more record to submit",
            icon_class="fa fa-info-circle",
            action="submit_assembly",
            icon_colour="teal",
        )
        delete_record_multi = RecordActionButton().create_record_action_button(
            name="delete_record_multi",
            title="Delete records",
            label="Delete",
            type="multi",
            error_message="Please select one or more records to delete",
            icon_class="fa fa-trash-can",
            action="validate_and_delete",
            icon_colour="red",
        )
        releasestudy = RecordActionButton().create_record_action_button(
            name="releasestudy",
            title="Release Study",
            label="Release Study",
            type="single",
            error_message="",
            icon_class="fa fa-globe",
            action="release_study",
            icon_colour="blue",
        )
        delete_read_multi = RecordActionButton().create_record_action_button(
            name="delete_read_multi",
            title="Delete records",
            label="Delete",
            type="multi",
            error_message="Please select one or more records to delete",
            icon_class="fa fa-trash-can",
            action="delete_read",
            icon_colour="red",
        )
        delete_singlecell_multi = RecordActionButton().create_record_action_button(
            name="delete_singlecell_multi",
            title="Delete records",
            label="Delete",
            type="multi",
            error_message="Please select one or more records to delete",
            icon_class="fa fa-trash-can",
            action="delete_singlecell",
            icon_colour="red",
        )
        submit_singlecell_single_ena = RecordActionButton().create_record_action_button(
            name="submit_singlecell_single_ena",
            title="Submit Single-cell Records to ENA",
            label="Submit to ENA",
            type="single",
            error_message="Please select one record to submit",
            icon_class="fa fa-info-circle",
            action="submit_singlecell_ena",
            icon_colour="teal",
        )
        submit_singlecell_single_zenodo = (
            RecordActionButton().create_record_action_button(
                name="submit_singlecell_single_zenodo",
                title="Submit Single-cell Records to ZENODO",
                label="Submit to ZENODO",
                type="single",
                error_message="Please select one record to submit",
                icon_class="fa fa-info-circle",
                action="submit_singlecell_zenodo",
                icon_colour="blue",
            )
        )
        publish_singlecell_single_ena = (
            RecordActionButton().create_record_action_button(
                name="publish_singlecell_single_ena",
                title="Publish Single-cell Records to ENA",
                label="Publish to ENA",
                type="single",
                error_message="Please select one record to publish",
                icon_class="fa fa-info-circle",
                action="publish_singlecell_ena",
                icon_colour="teal",
            )
        )
        publish_singlecell_single_zenodo = (
            RecordActionButton().create_record_action_button(
                name="publish_singlecell_single_zenodo",
                title="Publish Single-cell Records to ZENODO",
                label="Publish to ZENODO",
                type="single",
                error_message="Please select one record to publish",
                icon_class="fa fa-info-circle",
                action="publish_singlecell_zenodo",
                icon_colour="blue",
            )
        )

        make_snapshot = RecordActionButton().create_record_action_button(
            name="make_snapshot",
            title="Make Snapshot",
            label="Make Snapshot",
            type="single",
            error_message="Please select one record to make snapshot",
            icon_class="fa fa-camera-retro",
            action="make_snapshot",
            icon_colour="grey",
        )

        download_general_sample_manifest_single = RecordActionButton().create_record_action_button(
            name="download_general_sample_manifest_single",
            title="Download Sample Manifest",
            label="Download sample manifest",
            type="single",
            error_message="Please select one of samples in the manifest to download",
            icon_class="fa fa-download",
            action="download-sample-manifest",
            icon_colour="blue",
        )

        delete_sample_multi = RecordActionButton().create_record_action_button(
            name="delete_sample_multi",
            title="Delete records",
            label="Delete",
            type="multi",
            error_message="Please select one or more records to delete",
            icon_class="fa fa-trash-can",
            action="delete_sample",
            icon_colour="red",
        )

        submit_sample_multi = RecordActionButton().create_record_action_button(
            name="submit_general_sample_multi",
            title="Submit Sample to ENA",
            label="Submit to ENA",
            type="multi",
            error_message="Please select one or more record to submit",
            icon_class="fa fa-info-circle",
            action="submit_sample",
            icon_colour="teal",
        )

        self.stdout.write("Record Action Button Added")
        records = RecordActionButton.objects.all()

        for record in records:
            self.stdout.write(record.name)

        self.stdout.write("Removing Title Button ")
        TitleButton().remove_all_title_buttons()
        self.stdout.write("Adding Title Button ")

        accept_reject_samples = TitleButton().create_title_button(
            name="accept_reject_samples",
            template="<button style=\"display: none\" title=\"Accept/Reject TOL Samples\"             class=\"big circular ui icon teal button accept_reject_samples copo-tooltip\">         <i class=\"icon tasks sign\"></i>     </button>",
            additional_attr="",
        )
        tol_inspect = TitleButton().create_title_button(
            name="tol_inspect",
            template="<button style=\"display: none\" title=\"Inspect TOL\"             class=\"big circular ui icon yellow button tol_inspect copo-tooltip\">         <i class=\"icon clipboard list\"></i>     </button>",
            additional_attr="",
        )
        tol_inspect_gal = TitleButton().create_title_button(
            name="tol_inspect_gal",
            template="<button class=\"big circular ui icon green button tol_inspect_gal copo-tooltip\" title=\"Inspect TOL by GAL\">         <i class=\"icon building\"></i>     </button>",
            additional_attr="",
        )
        copo_accessions = TitleButton().create_title_button(
            name="copo_accessions",
            template="<button style=\"display: none\" title=\"View Accessions Dashboard\"             class=\"big circular ui icon pink button copo_accessions copo-tooltip\">         <i class=\"icon sitemap\"></i>     </button>",
            additional_attr="",
        )
        new_taggedseq_spreadsheet_template = TitleButton().create_title_button(
            name="new_taggedseq_spreadsheet_template",
            template="<button style=\"display: inline\" title=\"Add Tagged Sequence (s) from Tagged Sequence Spreadsheet\"             class=\"big circular ui icon button new-taggedseq-spreadsheet-template copo-tooltip\">         <i class=\"icon table sign\"></i>     </button>",
            additional_attr="",
        )
        new_terminal_file = TitleButton().create_title_button(
            name="new_terminal_file",
            template="<button title=\"Add new file by terminal\"             class=\"big circular ui icon primary button new-terminal-file copo-tooltip\">         <i class=\"icon terminal sign\"></i>     </button>",
            additional_attr="",
        )
        new_local_file = TitleButton().create_title_button(
            name="new_local_file",
            template="<button title=\"Add new file by browsing local file system\"             class=\"big circular ui icon primary button new-local-file copo-tooltip\">         <i class=\"icon desktop sign\"></i>     </button>",
            additional_attr="",
        )
        new_reads_spreadsheet_template = TitleButton().create_title_button(
            name="new_reads_spreadsheet_template",
            template="<button style=\"display: inline\" title=\"Add Read(s) from Read Spreadsheet\"             class=\"big circular ui icon button new-reads-spreadsheet-template copo-tooltip\">         <i class=\"icon table sign\"></i>     </button>",
            additional_attr="",
        )
        new_general_sample_spreadsheet_template = TitleButton().create_title_button(
            name="new_general_sample_spreadsheet_template",
            template="<button style=\"display: inline\" title=\"Add/Update Sample(s) from Sample Spreadsheet\"             class=\"big circular ui icon button new-general-sample-spreadsheet-template copo-tooltip\">         <i class=\"icon table sign\"></i>     </button>",
            additional_attr="",
        )
        new_singlecell_spreadsheet_template = TitleButton().create_title_button(
            name="new_singlecell_spreadsheet_template",
            template="<button style=\"display: inline\" title=\"Add study from Spreadsheet\"             class=\"big circular ui icon button new-singlecell-spreadsheet-template copo-tooltip\">         <i class=\"icon table sign\"></i>     </button>",
            additional_attr="",
        )
        new_samples_spreadsheet_template = TitleButton().create_title_button(
            name="new_samples_spreadsheet_template",
            template="<button   title=\"Add/Update sample(s) from spreadsheet\"             class=\"big circular ui icon button new-samples-spreadsheet-template copo-tooltip\">         <i class=\"icon table sign\"></i>     </button>",
            additional_attr="",
        )
        quick_tour_template = TitleButton().create_title_button(
            name="quick_tour_template",
            template="<button title=\"Quick tour\"             class=\"big circular ui icon orange button takeatour quick-tour-template copo-tooltip\">         <i class=\"icon lightbulb\"></i>     </button>",
            additional_attr="",
        )
        new_component_template = TitleButton().create_title_button(
            name="new_component_template",
            template="<button title=\"Add new profile record\"             class=\"big circular ui icon primary button new-component-template copo-tooltip\">         <i class=\"icon add sign\"></i>     </button>",
            additional_attr="",
        )
        download_sop = TitleButton().create_title_button(
            name="download_sop",
            template="<a title=\"Download Standard Operating Procedure (SOP)\"         class=\"big circular ui icon yellow button download-sop copo-tooltip\" target=\"_blank\">         <i class=\"icon download sign\"></i>     </a>",
            additional_attr="href:#sop_url",
        )
        download_blank_manifest_template = TitleButton().create_title_button(
            name="download_blank_manifest_template",
            template="<a  title=\"Download Blank Manifest Template\"             class=\"big circular ui icon brown button download-blank-manifest-template copo-tooltip\" target=\"_blank\">         <i class=\"icon download sign\"></i>     </a>",
            additional_attr="href:#blank_manifest_url",
        )

        self.stdout.write("Title Button Added")
        records = TitleButton.objects.all()

        for record in records:
            self.stdout.write(record.name)

        self.stdout.write("Setup Completed")

        self.stdout.write("Removing Component")
        Component().remove_all_components()
        self.stdout.write("Adding Component")

        # Components
        files = Component().create_component(
            name="files",
            title="Data Files",
            widget_icon="file",
            widget_colour="blue",
            widget_icon_class="fa fa-file",
            table_id="files_table",
            reverse_url="copo_file:copo_files",
            subtitle="",
        )

        sample = Component().create_component(
            name="sample",
            title="Samples",
            widget_icon="filter",
            widget_colour="olive",
            widget_icon_class="fa fa-filter",
            table_id="sample_table",
            reverse_url="copo_sample:copo_samples",
            subtitle="",
            button_label="Manage Sample metadata"
        )

        general_sample = Component().create_component(
            name="general_sample",
            title="Samples",
            widget_icon="filter",
            widget_colour="olive",
            widget_icon_class="fa fa-filter",
            table_id="sample_table",
            reverse_url="copo_sample:copo_general_samples",
            subtitle="#component_subtitle",
            button_label="Manage Sample metadata",
        )

        read = Component().create_component(
            name="read",
            title="Reads",
            widget_icon="dna",
            widget_colour="orange",
            widget_icon_class="fa fa-dna",
            table_id="read_table",
            reverse_url="copo_read_submission:copo_reads",
            subtitle="#component_subtitle",
        )
        
        reads_schema = Component().create_component(
            name="reads_schema",
            title="Reads",
            widget_icon="dna",
            widget_colour="orange",
            widget_icon_class="fa fa-dna",
            table_id="singlecell_table",
            reverse_url="copo_single_cell_submission:copo_singlecell",
            subtitle="#component_subtitle",
            schema_name="COPO_READ",
            base_component="singlecell",
        )
        
        singlecell = Component().create_component(
            name="singlecell",
            title="Single-cell",
            widget_icon="bacterium",
            widget_colour="green",
            widget_icon_class="fa fa-bacterium",
            table_id="singlecell_table",
            reverse_url="copo_single_cell_submission:copo_singlecell",
            subtitle="#component_subtitle",
            schema_name="COPO_SINGLE_CELL",
        )
        
        assembly = Component().create_component(
            name="assembly",
            title="Assembly",
            widget_icon="puzzle piece",
            widget_colour="violet",
            widget_icon_class="fa fa-puzzle-piece",
            table_id="assembly_table",
            reverse_url="copo_assembly_submission:copo_assembly",
            subtitle="",
        )

        seqannotation = Component().create_component(
            name="seqannotation",
            title="Sequence Annotations",
            widget_icon="tag",
            widget_colour="yellow",
            widget_icon_class="fa fa-tag",
            table_id="seqannotation_table",
            reverse_url="copo_seq_annotation_submission:copo_seq_annotation",
            subtitle="",
        )

        taggedseq = Component().create_component(
            name="taggedseq",
            title="Barcoding Manifests",
            widget_icon="barcode",
            widget_colour="red",
            widget_icon_class="fa fa-barcode",
            table_id="tagged_seq_table",
            reverse_url="copo_barcoding_submission:copo_taggedseq",
            subtitle="#component_subtitle",
        )

        images_rembi = Component().create_component(
            name="rembi",
            title="General",
            group_name="images",
            widget_icon="image",
            widget_colour="coral-pink",
            widget_icon_class="fa fa-image",
            table_id="singlecell_table",
            reverse_url="copo_single_cell_submission:copo_singlecell",
            subtitle="#component_subtitle",
            schema_name="COPO_IMAGE_REMBI",
            base_component="singlecell",
        )

        images_stx_fish = Component().create_component(
            name="stx_fish",
            title="Spatial Transcriptomics",
            group_name="images",
            widget_icon="image",
            widget_colour="terra-cotta",
            widget_icon_class="fa fa-image",
            table_id="singlecell_table",
            reverse_url="copo_single_cell_submission:copo_singlecell",
            subtitle="#component_subtitle",
            schema_name="COPO_IMAGE_STX_FISH",
            base_component="singlecell",
        )

        accessions = Component().create_component(
            name="accessions",
            title="Accessions",
            widget_icon="sitemap",
            widget_colour="pink",
            widget_icon_class="fa fa-sitemap",
            table_id="accessions_table",
            reverse_url="copo_accession:copo_accessions",
            subtitle="",
            button_label="View Accessions"
        )
        
        accessions_schema = Component().create_component(
            name="accessions_schema",
            title="Accessions",
            widget_icon="sitemap",
            widget_colour="pink",
            widget_icon_class="fa fa-sitemap",
            table_id="accessions_schema_table",
            reverse_url="copo_accessions_schema:copo_accessions_schema",
            subtitle="#component_subtitle",
            button_label="View Accessions",
        )
        
        profile = Component().create_component(
            name="profile",
            title="Work Profiles",
            widget_icon="",
            widget_colour="",
            widget_icon_class="",
            table_id="copo_profiles_table",
            reverse_url="",
            subtitle="#component_subtitle",
            button_label=""
        )
        
        # Assign record action buttons and title buttons to components
        assembly.recordaction_buttons.set(
            [
                add_record_all,
                edit_record_single,
                delete_record_multi,
                submit_assembly_multi,
            ]
        )
        assembly.title_buttons.set([new_component_template])

        taggedseq.recordaction_buttons.set(
            [
                add_record_all,
                edit_record_single,
                delete_record_multi,
                submit_tagged_seq_multi,
            ]
        )
        taggedseq.title_buttons.set(
            [new_taggedseq_spreadsheet_template, download_blank_manifest_template]
        )

        files.recordaction_buttons.set(
            [add_local_all, add_terminal_all, delete_record_multi]
        )
        files.title_buttons.set([new_local_file, new_terminal_file])

        seqannotation.recordaction_buttons.set(
            [
                add_record_all,
                edit_record_single,
                delete_record_multi,
                submit_annotation_multi,
            ]
        )
        seqannotation.title_buttons.set([new_component_template])

        general_sample.recordaction_buttons.set(
            [   
                download_general_sample_manifest_single,
                delete_sample_multi,
                submit_sample_multi,
            ]
        )
        general_sample.title_buttons.set(
            [new_general_sample_spreadsheet_template, download_blank_manifest_template]
        )

        read.recordaction_buttons.set([delete_read_multi, submit_read_multi])
        read.title_buttons.set(
            [new_reads_spreadsheet_template, download_blank_manifest_template]
        )

        singlecell.recordaction_buttons.set(
            [
                delete_singlecell_multi,
                download_singlecell_manifest_single,
            ]
        )
        singlecell.title_buttons.set(
            [new_singlecell_spreadsheet_template, download_blank_manifest_template]
        )

        reads_schema.recordaction_buttons.set(
            [
                delete_singlecell_multi,
                download_singlecell_manifest_single,
            ]
        )
        reads_schema.title_buttons.set(
            [new_singlecell_spreadsheet_template, download_blank_manifest_template]
        )

        images_rembi.recordaction_buttons.set(
            [delete_singlecell_multi, download_singlecell_manifest_single]
        )
        images_rembi.title_buttons.set(
            [new_singlecell_spreadsheet_template, download_blank_manifest_template]
        )

        images_stx_fish.recordaction_buttons.set(
            [delete_singlecell_multi, download_singlecell_manifest_single]
        )
        images_stx_fish.title_buttons.set(
            [new_singlecell_spreadsheet_template, download_blank_manifest_template]
        )

        sample.recordaction_buttons.set(
            [
                download_sample_manifest_single,
                download_permits_multiple,
                view_images_multiple,
            ]
        )
        sample.title_buttons.set(
            [
                quick_tour_template,
                new_samples_spreadsheet_template,
                download_blank_manifest_template,
                download_sop,
                accept_reject_samples,
            ]
        )

        accessions.title_buttons.set(
            [copo_accessions, accept_reject_samples, tol_inspect, tol_inspect_gal]
        )

        profile.recordaction_buttons.set([releasestudy])
        profile.title_buttons.set([quick_tour_template, new_component_template])

        self.stdout.write("Component Added")
        records = Component.objects.all()

        for record in records:
            self.stdout.write(record.name)

        self.stdout.write("Removing Existing Profile Types ")
        ProfileType().remove_all_profile_types()
        
        # Add Profile Types
        self.stdout.write("Adding Profile Types")

        erga = ProfileType().create_profile_type(
            type="erga",
            description="European Reference Genome Atlas (ERGA)",
            widget_colour="#E61A8D",
            is_dtol_profile=True,
            is_permission_required=True,
            post_save_action="src.apps.copo_profile.utils.profile_utils.post_save_dtol_profile",
            pre_save_action="src.apps.copo_profile.utils.profile_utils.pre_save_erga_profile",
        )
        asg = ProfileType().create_profile_type(
            type="asg",
            description="Aquatic Symbiosis Genomics (ASG)",
            widget_colour="#5829bb",
            is_dtol_profile=True,
            is_permission_required=True,
            post_save_action="src.apps.copo_profile.utils.profile_utils.post_save_dtol_profile",
        )
        dtolenv = ProfileType().create_profile_type(
            type="dtolenv",
            description="Darwin Tree of Life Environmental Samples (DTOLENV)",
            widget_colour="#fb7d0d",
            is_dtol_profile=True,
            is_permission_required=True,
        )
        dtol = ProfileType().create_profile_type(
            type="dtol",
            description="Darwin Tree of Life (DTOL)",
            widget_colour="#16ab39",
            is_dtol_profile=True,
            is_permission_required=True,
            post_save_action="src.apps.copo_profile.utils.profile_utils.post_save_dtol_profile",
        )
        genomics = ProfileType().create_profile_type(
            type="genomics",
            description="Genomics",
            widget_colour="#009c95",
            is_dtol_profile=False,
            is_permission_required=False,
            is_deprecated=True,
        )

        biodata = ProfileType().create_profile_type(
            type="biodata",
            description="Biodata",
            widget_colour="#00AAFF",
            is_dtol_profile=False,
            is_permission_required=False,
        )
        
        # Assign components to profile types
        erga.components.set(
            [files, sample, read, assembly, seqannotation, taggedseq, accessions]
        )
        asg.components.set(
            [files, sample, read, assembly, seqannotation, taggedseq, accessions]
        )
        dtolenv.components.set(
            [files, sample, read, assembly, seqannotation, taggedseq, accessions]
        )
        dtol.components.set(
            [files, sample, read, assembly, seqannotation, taggedseq, accessions]
        )
        genomics.components.set(
            [
                files,
                general_sample,
                read,
                assembly,
                seqannotation,
                accessions,
            ]
        )
        biodata.components.set(
            [
                files,
                general_sample,
                reads_schema,
                singlecell,
                images_rembi,
                images_stx_fish,
                accessions_schema,
            ]
        )

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

        erga.associated_profile_types.set(
            [
                at_bge,
                at_bioblitz,
                at_cbp,
                at_erga_pilot,
                at_erga_community,
                at_pop_genomics,
                at_sanger,
            ]
        )
        asg.associated_profile_types.set([at_asg])
        dtolenv.associated_profile_types.set([at_dtolenv])
        dtol.associated_profile_types.set([at_dtol])

        self.stdout.write("Profile Types Added")
        records = ProfileType.objects.all()

        for record in records:
            self.stdout.write(record.type)

        # refresh the schema in case it changes the schema
        DataSchemas.refresh()
