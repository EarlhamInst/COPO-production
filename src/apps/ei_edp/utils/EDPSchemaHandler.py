from common.utils.logger import Logger
import pandas as pd
import os
import uuid
from django.conf import settings
from openpyxl.utils.cell import get_column_letter
from openpyxl import load_workbook
from openpyxl.styles import (
    Font,
    NamedStyle,
    PatternFill,
    Alignment,
    Border,
    Side,
    Protection,
)
from openpyxl.comments import Comment
from openpyxl.worksheet.datavalidation import DataValidation
from common.dal.profile_da import Profile
from src.apps.copo_single_cell_submission.utils.da import SinglecellSchemas
from src.apps.copo_single_cell_submission.utils.SingleCellSchemasHandler import (
    SingleCellSchemasHandler,
    SinglecellschemasSpreadsheet,
)
from .lims import get_lims_adapter
from .constants import (
    BIOSAMPLE_ACCESSION,
    CHOICE,
    USER_EMAIL,
    EDP_SCHEMA_NAME,
    HEALTH_SAFETY,
    LIMS_HEALTH_AND_SAFETY,
    LIMS_SAMPLE_RETURN,
    PROFILE_BUDGET_HOLDER,
    PROFILE_SAPIO_PROJECT_ID,
    SAMPLE_ID,
    SAMPLE_RETURN,
    SCIENTIFIC_NAME,
    STUDY_ID,
    SUBMITTER_SAMPLE_REFERENCE,
    TAXON_ID,
    TERM_ACCESS,
    TERM_DESCRIPTION,
    TERM_LABEL,
    TERM_LENGTH,
    TERM_NAME,
    TERM_TYPE,
)
from .edp_validation_messages import MESSAGES
from openpyxl.utils.dataframe import dataframe_to_rows
from common.ena_utils.generic_helper import notify_singlecell_status
from openpyxl.worksheet.formula import ArrayFormula


def get_submitted_sample_ids(project_id, schemas):
    '''This function returns the IDs of samples from a Sapio LIMS project 
    that have already been submitted.

    A sample counts as being submitted once its `Submitter Sample Reference` has been
    written to the LIMS (in Sapio, it is called `C_CustomerSampleName`). It is left out of
    new manifests and kept when a later manifest is uploaded without it.
    '''
    return set(get_submitted_sample_references(project_id, schemas))


def get_submitted_sample_references(project_id, schemas):
    '''This function maps the ID of each submitted sample in a Sapio LIMS
    project to its `Submitter Sample Reference`.

    A submitted sample can be uploaded again to update its details in the
    LIMS but, its `Submitter Sample Reference` must stay the same.
    '''
    if not project_id:
        return {}
    return {
        str(record[SAMPLE_ID]): str(record[SUBMITTER_SAMPLE_REFERENCE]).strip()
        for record in get_lims_adapter().get_project_samples(project_id, schemas)
        if record.get(SAMPLE_ID) and record.get(SUBMITTER_SAMPLE_REFERENCE)
    }


def get_pending_samples(project_id, schemas):
    '''This function returns the samples from a Sapio LIMS project
    that are yet to be submitted. These are the samples that a 
    downloaded blank manifest will list.
    '''
    return [
        record
        for record in get_lims_adapter().get_project_samples(project_id, schemas)
        if not record.get(SUBMITTER_SAMPLE_REFERENCE)
    ]


class EDPSchemaHandler(SingleCellSchemasHandler):
    sample_name_field = SUBMITTER_SAMPLE_REFERENCE

    def write_manifest(
        self,
        singlecell_schema,
        profile_id=None,
        checklist_id=None,
        singlecell=None,
        file_path=None,
        format="xlsx",
        request=None,
    ):
        schema_name = singlecell_schema["name"]
        schemas = singlecell_schema["schemas"]
        checklists = singlecell_schema["checklists"]
        # component_names = singlecell_schema["components"]

        profile = Profile().get_record(profile_id)
        if not profile:
            raise Exception(MESSAGES['profile_not_found'].format(profile_id=profile_id))

        sapio_project_id = profile.get(PROFILE_SAPIO_PROJECT_ID, None)
        
        # Sample rows in LIMS manifests originate from the Sapio project so, without
        # Sapio project, there is nothing for the user to fill in.
        if not sapio_project_id:
            raise Exception(MESSAGES['missing_sapio_project'])

        # Existing LIMS samples (keyed by COPO term_name) to pre-fill the sample
        # sheet, plus study-level values for the manifest header.
        # A blank manifest will only list samples that are yet to be
        # submitted like when the sample count on a profile has increased.
        # Submitted samples can be updated by uploading their original
        # manifest again with amendments.
        lims_sample_records = get_pending_samples(sapio_project_id, schemas)
        if not lims_sample_records:
            raise Exception(MESSAGES['all_samples_submitted'])
        lims_project_metadata = get_lims_adapter().get_project_metadata(sapio_project_id)

        # Cell formats
        alignment = Alignment(wrap_text=True, vertical="center", horizontal="center")
        font = Font(bold=True)
        bolder = Border(
            left=Side(style='medium'),
            right=Side(style='medium'),
            top=Side(style='medium'),
            bottom=Side(style='medium'),
        )
        mandatory_style = NamedStyle(name="mandatory_style")
        mandatory_style.font = font
        mandatory_style.fill = PatternFill(fgColor="A6C875", fill_type="solid")
        mandatory_style.alignment = alignment
        mandatory_style.border = bolder
        optional_style = NamedStyle(name="optional_style")
        optional_style.font = font
        optional_style.fill = PatternFill(fgColor="90D5FF", fill_type="solid")
        optional_style.alignment = alignment
        optional_style.border = bolder
        protected_style = NamedStyle(name="protected_style")
        protected_style.font = font
        protected_style.fill = PatternFill(fgColor="D3D3D3", fill_type="solid")
        protected_style.alignment = alignment
        protected_style.border = bolder
        component_title_style = NamedStyle(name="component_title_style")
        component_title_style.font = Font(bold=True, size=16)
        component_title_style.alignment = Alignment(
            horizontal="center", vertical="center"
        )
        component_title_style.border = bolder
        component_title_style.fill = PatternFill(fgColor="FFBF00", fill_type="solid")

        version = settings.MANIFEST_VERSION.get(schema_name, str())
        if version:
            version = "_v" + version

        workbook = load_workbook(settings.SINGLE_CELL_SCHEMAS_TEMPLATE_URL[EDP_SCHEMA_NAME])
        worksheet_sample = workbook["sample"]

        worksheet_sample["C4"] = profile.get(PROFILE_BUDGET_HOLDER, "")
        worksheet_sample["C5"] = profile.get(PROFILE_SAPIO_PROJECT_ID, "")

        if lims_project_metadata:
            worksheet_sample["L9"] = lims_project_metadata.get(LIMS_HEALTH_AND_SAFETY)
            worksheet_sample["L9"].protection = Protection(locked=False)
            if LIMS_SAMPLE_RETURN in lims_project_metadata:
                worksheet_sample["L8"] = lims_project_metadata.get(LIMS_SAMPLE_RETURN)
                worksheet_sample["L8"].protection = Protection(locked=False)

        worksheet_helper = workbook["How to complete the manifest"]
        worksheet_helper.protection.sheet = True

        for checklist in checklists.keys():
            if checklist_id and checklist_id != checklist:
                continue

            schema_checklist = checklist
            if not checklist_id or file_path is None:
                file_path = os.path.join(
                    settings.MANIFEST_PATH,
                    settings.MANIFEST_FILE_NAME.format(
                        schema_name + "_" + checklist, version
                    ),
                )

            worksheet_helper_current_row = 4
            submitter_sample_reference = []
            for component_name, schema in schemas.items():

                component_data_df = pd.DataFrame()

                if singlecell is not None:
                    component_data_df = pd.DataFrame.from_records(
                        singlecell["components"].get(component_name, [])
                    )

                worksheet_helper_current_row += 1

                component_schema_df = pd.DataFrame.from_records(schema)
                component_schema_df = component_schema_df.drop(
                    component_schema_df[
                        pd.isna(component_schema_df[schema_checklist])
                    ].index
                )
                if component_schema_df.empty:
                    continue

                # pandas 3 refuses to fill a float64 column with "" (raises
                # TypeError); cast to object first so blank cells fill cleanly.
                component_schema_df = component_schema_df.astype(object).fillna("")
                component_schema_df[CHOICE] = component_schema_df[
                    component_schema_df[TERM_TYPE].isin(["enum", "suggested_enum"])
                ][TERM_NAME].apply(
                    lambda x: singlecell_schema.get("enums", []).get(x, [])
                )

                cell_component_title = worksheet_helper.cell(
                    row=worksheet_helper_current_row, column=1
                )
                cell_component_title.value = component_name.upper() + " DETAILS"
                cell_component_title.style = component_title_style
                cell_component_title_blank = worksheet_helper.cell(
                    row=worksheet_helper_current_row, column=2
                )
                cell_component_title_blank.border = bolder

                worksheet_helper_current_row += 1

                for _, field in component_schema_df.iterrows():

                    if field[TERM_ACCESS] == "protected":
                        continue

                    label = field[TERM_LABEL]
                    description = field.get(TERM_DESCRIPTION, label)

                    cell = worksheet_helper.cell(
                        row=worksheet_helper_current_row, column=1
                    )
                    cell.value = label
                    cell.alignment = Alignment(
                        wrap_text=True, vertical="top", horizontal="left"
                    )
                    if field[schema_checklist] == "M":
                        cell.style = mandatory_style
                    elif field[schema_checklist] == "O":
                        cell.style = optional_style
                    if field[TERM_ACCESS] == "protected":
                        cell.style = protected_style

                    cell_desc = worksheet_helper.cell(
                        row=worksheet_helper_current_row, column=2
                    )
                    cell_desc.value = description
                    cell_desc.alignment = Alignment(
                        wrap_text=True, vertical="top", horizontal="left"
                    )
                    cell_desc.border = bolder

                    worksheet_helper_current_row += 1

                if component_name == "sample":
                    sapio_component_data_df = pd.DataFrame.from_records(
                        lims_sample_records
                    )

                    worksheet_sample = workbook.create_sheet("sample_metadata")
                    worksheet_sample.protection.sheet = True
                    sample_metadata_columns = [
                        SUBMITTER_SAMPLE_REFERENCE,
                        TAXON_ID,
                        SCIENTIFIC_NAME,
                        BIOSAMPLE_ACCESSION,
                    ]
                    submitter_sample_reference = []
                    worksheet_sample.append(sample_metadata_columns)
                    if not component_data_df.empty:
                        for r in dataframe_to_rows(
                            component_data_df[sample_metadata_columns],
                            index=False,
                            header=False,
                        ):
                            worksheet_sample.append(r)
                        submitter_sample_reference = component_data_df[
                            SUBMITTER_SAMPLE_REFERENCE
                        ].tolist()
                    component_data_df = sapio_component_data_df

                # Individual libraries have one index per sample so, the `Sample ID`
                # is prefilled with a row for each LIMS sample. For a pool of libraries,
                # the user overwrites it with the `Sample ID` of the pool.
                if component_name == "index" and component_data_df.empty:
                    component_data_df = pd.DataFrame(
                        {SAMPLE_ID: [record.get(SAMPLE_ID, "") for record in lims_sample_records]}
                    )

                if component_name == "study":
                    continue

                title_row = 1
                if component_name == "sample":
                    worksheet = workbook[component_name]
                    title_row = 28
                else:
                    worksheet = workbook.create_sheet(component_name)
                    title_row = 1

                worksheet.protection.sheet = True
                column_index = 0
                # column holding submitter_sample_reference (the VLOOKUP key);
                # captured in-loop so the key column is not hardcoded
                reference_column_letter = "I"
                for _, field in component_schema_df.iterrows():
                    column_index += 1
                    if field[TERM_NAME] == SUBMITTER_SAMPLE_REFERENCE:
                        reference_column_letter = get_column_letter(column_index)
                    name = field[TERM_LABEL]
                    cell = worksheet.cell(row=title_row, column=column_index)
                    cell.value = name
                    description = field.get(TERM_DESCRIPTION, name)
                    if description:
                        comment = Comment(field.get(TERM_DESCRIPTION, name), "COPO")
                        cell.comment = comment
                    if field[checklist] == "M":
                        cell.style = mandatory_style
                    elif field[checklist] == "O":
                        cell.style = optional_style
                    if field[TERM_ACCESS] == "protected":
                        cell.style = protected_style
                        if title_row > 1:
                            cell_highlight = worksheet.cell(
                                row=title_row - 1, column=column_index
                            )
                            cell_highlight.fill = protected_style.fill
                    elif field[TERM_ACCESS] == "hidden":
                        worksheet.column_dimensions[
                            get_column_letter(column_index)
                        ].hidden = True
                    else:
                        # unprotect the cell of the column
                        # Sample rows come from the LIMS but, a pool of libraries can have
                        # more indexes than samples so, the other worksheets unprotect the
                        # same rows as the data validations
                        unprotected_row_count = (
                            len(lims_sample_records)
                            if component_name == "sample"
                            else 1000 - title_row
                        )
                        for i in range(1, unprotected_row_count + 1):
                            cell_under_column = worksheet.cell(
                                row=title_row + i, column=column_index
                            )
                            cell_under_column.protection = Protection(locked=False)

                    worksheet.column_dimensions[
                        get_column_letter(column_index)
                    ].width = 17.83

                    type = field.get(TERM_TYPE, "string")
                    formula1 = None
                    if (
                        type in ["enum", "suggested_enum"]
                        or field[TERM_NAME] == SUBMITTER_SAMPLE_REFERENCE
                    ):
                        # Create a data-validation object with list validation
                        if field[TERM_NAME] == SUBMITTER_SAMPLE_REFERENCE:
                            # options = submitter_sample_reference
                            # av_range = DefinedName('submitter_sample_reference', attr_text="sample_metadata!$A$2:$A$8")
                            # worksheet.defined_names.add(av_range)
                            formula1 = 'sample_metadata!$A$2:$A$1000'

                        else:
                            options = field[CHOICE]
                            if options:
                                formula1 = '"' + ",".join(options) + '"'

                        if formula1:
                            dv = DataValidation(
                                type="list",
                                formula1=formula1,
                                allow_blank=True,
                                # prompt = description if description else "",
                            )
                            # Add the data-validation object to the worksheet
                            cell_start_end = '%s%d:%s%d' % (
                                get_column_letter(column_index),
                                title_row + 1,
                                get_column_letter(column_index),
                                1000,
                            )
                            worksheet.add_data_validation(dv)
                            dv.add(cell_start_end)
                    elif type == "string":
                        length = field[TERM_LENGTH]
                        if length and int(length) > 0:
                            cell_start_end = '%s%d:%s%d' % (
                                get_column_letter(column_index),
                                title_row + 1,
                                get_column_letter(column_index),
                                1000,
                            )
                            dv = DataValidation(
                                type="textLength",
                                operator="lessThanOrEqual",
                                formula1=str(length),
                                allow_blank=True,
                                prompt="Please input the value with length less than or equal to "
                                + str(length),
                                # TBC stop on error
                            )
                            # Add the data-validation object to the worksheet
                            worksheet.add_data_validation(dv)
                            dv.add(cell_start_end)

                    # VLOOKUP column indices into the sample_metadata table (A=1 key, B=2, C=3, D=4)
                    lookup_mapping = {
                        TAXON_ID: 2,
                        SCIENTIFIC_NAME: 3,
                        BIOSAMPLE_ACCESSION: 4,
                    }

                    data_row_index = title_row
                    for _, row in component_data_df.iterrows():
                        data_row_index += 1
                        # Lookup columns are filled in by an array formula as shown below below
                        if field[TERM_NAME] in lookup_mapping:
                            continue
                        cell = worksheet.cell(row=data_row_index, column=column_index)
                        cell.value = row.get(field[TERM_NAME], "")

                    if field[TERM_NAME] in lookup_mapping.keys():
                        column_letter = get_column_letter(column_index)
                        # exact-match VLOOKUP keyed on the submitter_sample_reference
                        # column (not a hardcoded 'I'), against the sample_metadata table
                        lookup_index = lookup_mapping[field[TERM_NAME]]
                        reference_range = f"{reference_column_letter}{title_row + 1}:{reference_column_letter}{data_row_index}"
                        worksheet[f"{column_letter}{title_row + 1}"] = ArrayFormula(
                            f"{column_letter}{title_row + 1}:{column_letter}{data_row_index}",
                            # An appropriate error message is shown to users when a value for the 
                            # `submitter_sample_reference` field is missing or unknown
                            # rather than the default and automatic value `#N/A`
                            # NB: Double quotes must be used in spreadsheet formulas not single quotes.
                            f'=IF({reference_range}="","{MESSAGES["lookup_missing_submitter_sample_reference"]}",'
                            f'IFERROR(VLOOKUP({reference_range},sample_metadata!$A:$D,{lookup_index},FALSE),'
                            f'"{MESSAGES["lookup_unknown_submitter_sample_reference"]}"))',
                        )

        # Move the `sample_metadata` worksheet to the end
        if 'sample_metadata' in workbook.sheetnames:
            workbook.move_sheet(
                'sample_metadata',
                offset=len(workbook.sheetnames) - 1 - workbook.sheetnames.index('sample_metadata'),
            )

        # The `openpyxl` library cannot evaluate formulas so the spreadsheet application
        # is asked to recalculate on open to prevent the `VLOOKUP` formula cells from showing no value
        workbook.calculation.fullCalcOnLoad = True

        # Password-protect locked sheets with a random password
        # so that users cannot unlock them to edit locked cells
        sheet_password = uuid.uuid4().hex
        for worksheet in workbook.worksheets:
            if worksheet.protection.sheet:
                worksheet.protection.password = sheet_password

        workbook.save(file_path)

    def carry_over_existing_rows(self, profile_id, schemas, existing_record, singlecell_record):
        # Keep the rows of samples that have already been submitted to the Sapio's LIMS that a
        # downloaded manifest leaves out after the sample count has been increased
        for component_name, existing_component_data in existing_record['components'].items():
            component_data_df = singlecell_record['components'].get(component_name)
            
            if component_data_df is None or not existing_component_data:
                continue
            rows_to_keep_df = self._get_rows_to_keep(
                profile_id,
                schemas,
                pd.DataFrame.from_records(existing_component_data),
                component_data_df,
            )
            if rows_to_keep_df.empty:
                continue
            
            # Merge the manifest's columns back into the existing record
            rows_to_keep_df = rows_to_keep_df[
                [c for c in component_data_df.columns if c in rows_to_keep_df.columns]
            ]
            singlecell_record['components'][component_name] = (
                pd.concat([rows_to_keep_df, component_data_df], ignore_index=True)
                .astype(object)
                .fillna('')
            )

    def _get_rows_to_keep(self, profile_id, schemas, existing_data_df, new_data_df):
        # Get rows of samples that already exist and have been submitted already
        # to Sapio's LIMS that a manifest upload tends to leave out
        if SAMPLE_ID not in existing_data_df.columns:
            return existing_data_df.iloc[0:0]

        if not hasattr(self, '_submitted_sample_ids'):
            profile = Profile().get_record(profile_id) or {}
            self._submitted_sample_ids = get_submitted_sample_ids(
                profile.get(PROFILE_SAPIO_PROJECT_ID, ''), schemas
            )
            
        sample_ids = existing_data_df[SAMPLE_ID].astype(str)
        uploaded_sample_ids = (
            set(new_data_df[SAMPLE_ID].astype(str))
            if SAMPLE_ID in new_data_df.columns
            else set()
        )
        
        return existing_data_df[
            sample_ids.isin(self._submitted_sample_ids)
            & ~sample_ids.isin(uploaded_sample_ids)
        ]


class EDPSchemaSpreadsheet(SinglecellschemasSpreadsheet):
    def loadManifest(self, m_format):
        self.data = {}
        if self.profile_id is not None:
            notify_singlecell_status(
                data={"profile_id": self.profile_id},
                msg="Loading...",
                action="info",
                html_id=self.component_info,
                checklist_id=self.checklist_id,
            )
            workbook = load_workbook(self.file, data_only=True)
            profile = Profile().get_record(self.profile_id)
            sample_ws = workbook["sample"]
            sapio_project_id = str(sample_ws["C5"].value)
            if sample_ws["E6"].value:
                emails = str(sample_ws["E6"].value).strip()
            else:
                emails = ""
            is_returned = sample_ws["L8"].value
            health_safety = sample_ws["L9"].value

            if not sapio_project_id or sapio_project_id != profile.get(
                PROFILE_SAPIO_PROJECT_ID, ""
            ):
                notify_singlecell_status(
                    data={"profile_id": self.profile_id},
                    msg=(
                        "Incorrect project ID: " + str(sapio_project_id)
                        if sapio_project_id
                        else "Missing project ID in the manifest!"
                    ),
                    action="error",
                    html_id=self.component_info,
                    checklist_id=self.checklist_id,
                )
                return False

            study_dict = [
                {
                    STUDY_ID: (
                        profile[PROFILE_SAPIO_PROJECT_ID]
                        if PROFILE_SAPIO_PROJECT_ID in profile
                        else ""
                    ),
                    USER_EMAIL: emails,
                    SAMPLE_RETURN: is_returned,
                    HEALTH_SAFETY: health_safety,
                }
            ]
            study_df = pd.DataFrame.from_records(study_dict)
            self.data["study"] = study_df
            self.new_data["study"] = study_df

            for sheetname in workbook.sheetnames:
                if sheetname in ["How to complete the manifest", "sample_metadata"]:
                    continue
                if sheetname not in self.schemas:
                    notify_singlecell_status(
                        data={"profile_id": self.profile_id},
                        msg="Incorrect worksheet in the file: " + sheetname,
                        action="error",
                        html_id=self.component_info,
                        checklist_id=self.checklist_id,
                    )
                    return False
                component_ws = workbook[sheetname]
                if sheetname == "sample":
                    component_data = self._get_ws_rows(component_ws, min_row=28)
                else:
                    component_data = self._get_ws_rows(component_ws, min_row=1)
                cols = next(component_data)
                component_data = list(component_data)
                component_df = pd.DataFrame(component_data, columns=cols)
                new_column_name = {
                    item[TERM_LABEL]: key
                    for key, item in self.schemas[sheetname].items()
                }
                component_df["Project ID"] = (
                    profile[PROFILE_SAPIO_PROJECT_ID] if PROFILE_SAPIO_PROJECT_ID in profile else ""
                )
                component_df = component_df.loc[
                    :, ~component_df.columns.str.contains('^Unnamed')
                ]
                component_df = component_df.astype(object).fillna("")
                
                # The lookup columns might show a prompt instead of a value when the
                # `submitter_sample_reference` field is missing a value or it is unknown
                # If this occurs, the value is treated as blank and the prompt text
                # is never saved as the metadata for the sample.
                component_df = component_df.replace(
                    [
                        MESSAGES['lookup_missing_submitter_sample_reference'],
                        MESSAGES['lookup_unknown_submitter_sample_reference'],
                    ],
                    '',
                )
                self.data[sheetname] = component_df
                self.new_data[sheetname] = component_df.rename(columns=new_column_name)

            # Rows for samples already submitted to the LIMS can be uploaded
            # again to update their details in the LIMS. Each row is matched
            # to its LIMS sample by `Sample ID` but, the `Submitter Sample Reference`
            # that marked the sample as submitted must not change
            sample_df = self.new_data.get("sample")
            if (
                sample_df is not None
                and {SAMPLE_ID, SUBMITTER_SAMPLE_REFERENCE} <= set(sample_df.columns)
            ):
                schemas = SinglecellSchemas().get_schema(
                    schema_name=self.schema_name, target_id=self.checklist_id
                )
                submitted_references = get_submitted_sample_references(
                    sapio_project_id, schemas
                )
                changed_references = sorted(
                    str(sample_id)
                    for sample_id, reference in zip(
                        sample_df[SAMPLE_ID], sample_df[SUBMITTER_SAMPLE_REFERENCE]
                    )
                    if str(sample_id) in submitted_references
                    and str(reference).strip() != submitted_references[str(sample_id)]
                )
                if changed_references:
                    notify_singlecell_status(
                        data={"profile_id": self.profile_id},
                        msg=MESSAGES['submitter_sample_reference_changed'].format(
                            sample_ids=", ".join(changed_references)
                        ),
                        action="error",
                        html_id=self.component_info,
                        checklist_id=self.checklist_id,
                    )
                    return False
            """
            if "index" in workbook.sheetnames:
                index_ws = workbook["index"]
                index_data = self._get_ws_rows(index_ws, min_row=1)
                cols = next(index_data)
                index_data = list(index_data)
                index_df = pd.DataFrame(index_data,columns=cols)
                new_column_name = {
                    item[TERM_LABEL]: key
                    for key, item in self.schemas["index"].items()
                }
                index_df["Project ID"]= profile[PROFILE_SAPIO_PROJECT_ID] if PROFILE_SAPIO_PROJECT_ID in profile else ""
                index_df = index_df.astype(object).fillna("")
                self.data["index"] = index_df
                self.new_data["index"] = index_df.rename(columns=new_column_name)
            """
        return True

    def _get_ws_rows(self, worksheet, min_row=1):
        for row in worksheet.iter_rows(min_row=min_row, values_only=True):
            yield row
