from common.dal.sample_da import Sample
from common.dal.profile_da import Profile
from common.utils.helpers import notify_frontend
from common.validators.validator import Validator
from .validation_messages import MESSAGES as msg
from collections import Counter
from common.schema_versions.lookup.dtol_lookups import BLANK_VALS, \
    NA_VALS, POP_GENOMICS_OPTIONAL_COLUMNS_DEFAULT_VALUES_MAPPING, SLASHES_LIST


class ColumnValidator(Validator):
    def validate(self):
        p_type = Profile().get_type(profile_id=self.profile_id)
        columns = list(self.data.columns)

        notify_frontend(data={"profile_id": self.profile_id}, msg="Validating columns",
                action="info",
                html_id="sample_info")
        
        # check required fields are present in spreadsheet
        for item in self.fields:
            if item not in columns:
                # invalid or missing field, inform user and return false
                self.errors.append("Field not found - " + item)
                self.flag = False
                # if we have a required fields, check that there are no missing values
        
        notify_frontend(data={"profile_id": self.profile_id}, msg="Validating columns: Finished",
            action="info",
            max_ellipsis_length=0,
            html_id="sample_info")
                
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")


class CellMissingDataValidator(Validator):
    def validate(self):
        p_type = Profile().get_type(profile_id=self.profile_id)
        associated_p_type_lst = Profile().get_associated_type(
            self.profile_id, value=True, label=False)

        for header, cells in self.data.items():
            # here we need to check if there are not missing values in its cells
            if header in self.fields:
                if header == "SYMBIONT" and any(x in p_type for x in ["ASG","DTOL", "ERGA"]):
                    # 'ASG', 'DTOL' and 'ERGA' manifests should be autofilled with "TARGET" 
                    # if the 'SYMBIONT' field is left blank as of manifest v2.5
                    # This is handled by another validator
                    pass
                elif header in POP_GENOMICS_OPTIONAL_COLUMNS_DEFAULT_VALUES_MAPPING \
                        and "ERGA" in p_type  \
                        and ( "POP_GENOMICS" in associated_p_type_lst  \
                             and "BGE" in associated_p_type_lst) :
                        # erga manifests that inlude "POP_GENOMICS" as an associated tol project type
                        # are allowed to have blank field
                    pass
                else:
                    cellcount = 0
                    for c in cells:
                        cellcount += 1
                        if not c.strip():
                            # we have missing data in required cells
                            if header == "SYMBIONT":
                                self.errors.append(msg["validation_msg_missing_symbiont"] % (
                                    header, str(cellcount + 1), "TARGET and SYMBIONT"))
                            else:
                                self.errors.append(msg["validation_msg_missing_data"] % (
                                    header, str(cellcount + 1), BLANK_VALS))
                            self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")


class RackTubeNotNullValidator(Validator):
    def validate(self):
        for index, row in self.data.iterrows():
            if row.get("RACK_OR_PLATE_ID", "") in BLANK_VALS and row["TUBE_OR_WELL_ID"] in BLANK_VALS:
                self.errors.append(
                    msg["validation_msg_rack_tube_both_na"] % (str(index + 1)))
                self.flag = False

            if row.get("RACK_OR_PLATE_ID", "") in BLANK_VALS or row["TUBE_OR_WELL_ID"] in BLANK_VALS:
                self.errors.append(
                    msg["validation_msg_rack_or_tube_is_na"] % (str(index + 1)))
                self.flag = False

            if row.get("RACK_OR_PLATE_ID", "") in NA_VALS or row["TUBE_OR_WELL_ID"] in NA_VALS:
                self.errors.append(
                    msg["validation_msg_rack_or_tube_is_na"] % (str(index + 1)))
                self.flag = False

        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")


class OrphanedSymbiontValidator(Validator):
    def validate(self):
        # check that if sample is a symbiont, there is a target with matching RACK_OR_PLATE_ID and TUBE_OR_WELL_ID
        syms = self.data.loc[(self.data["SYMBIONT"] == "SYMBIONT")]
        if not syms.empty:
            tid = syms["TUBE_OR_WELL_ID"]
            for el in list(tid):
                target = self.data.loc[(self.data["SYMBIONT"] == "TARGET") & (
                    self.data["TUBE_OR_WELL_ID"] == el)]
                if len(target) == 0:
                    self.errors.append(
                        msg["validation_msg_orphaned_symbiont"] % el)
                    self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")


class RackPlateUniquenessValidator(Validator):
    def validate(self):
        """
        existing_samples = Sample().get_by_field("profile_id", [self.profile_id])
          
        # Check if samples already exist in the profile with the same RACK_OR_PLATE_ID and/ TUBE_OR_WELL_ID
        # if they do, display an 
        # This validation step handles when a manifest is downloaded and then uploaded again for the same profile
        if existing_samples:
            existing_rack_or_plate_ids = [sample.get("RACK_OR_PLATE_ID",'') for sample in existing_samples]
            existing_tube_or_well_ids = [sample.get("TUBE_OR_WELL_ID",'') for sample in existing_samples]

            if any(x in value for value in list(self.data.get("RACK_OR_PLATE_ID", "")) for x in existing_rack_or_plate_ids) or \
                any(x in value for value in list(self.data.get("TUBE_OR_WELL_ID", "")) for x in existing_tube_or_well_ids):

                for index, row in self.data.iterrows():
                    if row.get("RACK_OR_PLATE_ID", "") and row.get("TUBE_OR_WELL_ID", "") and \
                        row.get("RACK_OR_PLATE_ID", "") in existing_rack_or_plate_ids and \
                            row.get("TUBE_OR_WELL_ID", "") in existing_tube_or_well_ids:
                        self.errors.append(
                            msg["validation_msg_duplicate_tube_or_well_id_in_copo"] % (row.get("RACK_OR_PLATE_ID", "") + "/" + row.get("TUBE_OR_WELL_ID", "")))
                        self.flag = False
                    elif row.get("RACK_OR_PLATE_ID", "") and row.get("RACK_OR_PLATE_ID", "") in existing_rack_or_plate_ids:
                        self.errors.append(
                           msg["validation_msg_duplicate_rack_or_plate_id"] % (row.get("RACK_OR_PLATE_ID", "")))
                        self.flag = False
                    elif row.get("TUBE_OR_WELL_ID", "") and row.get("TUBE_OR_WELL_ID", "") in existing_tube_or_well_ids:
                        self.errors.append(
                            msg["validation_msg_duplicate_tube_or_well_id"] % (row.get("TUBE_OR_WELL_ID", "")))
                        self.flag = False
        """              
        # check for uniqueness of RACK_OR_PLATE_ID and TUBE_OR_WELL_ID in this manifest
        # RACK_OR_PLATE_ID and TUBE_OR_WELL_ID cannot contain any slashes i.e. '/' nor '\'
        if any(slash in value for value in list(self.data.get("RACK_OR_PLATE_ID", "")) for slash in SLASHES_LIST) or \
                any(slash in value for value in list(self.data.get("TUBE_OR_WELL_ID", "")) for slash in SLASHES_LIST):
            self.errors.append(
                msg["validation_msg_rack_or_tube_contains_a_slash"])
            self.flag = False
        else:
            rack_tube = self.data.get(
                "RACK_OR_PLATE_ID", "") + "/" + self.data["TUBE_OR_WELL_ID"]

            # now check for uniqueness across all Samples
            p_type = Profile().get_type(profile_id=self.profile_id)
            dup = Sample().check_dtol_unique(rack_tube)
            # duplicated returns a boolean array, false for not duplicate, true for duplicate
            u = list(rack_tube[rack_tube.duplicated()])

            if len(dup) > 0:
                # errors = list(map(lambda x: "<li>" + x + "</li>", errors))
                err = list(map(lambda x: x.get("RACK_OR_PLATE_ID",
                                               "") + "/" + x["TUBE_OR_WELL_ID"], dup))

                # check if rack_tube present we are in the same profile
                #existingsam = Sample().get_by_field(
                #    "rack_tube", err)  # [str(rack_tube[0])])
                existingsam = Sample().get_all_records_columns(filter_by={"rack_tube": {"$in": err}}, projection={"profile_id":1, "rack_tube":1, "biosampleAccession":1})
                for exsam in existingsam:
                    if exsam["profile_id"] == self.profile_id:
                        self.kwargs["isupdate"] = True
                        # todo check SYMBIONT value in species list is the same too
                        # check accessions do not exist yet and status is pending
                        if not exsam["biosampleAccession"]:
                            self.warnings.append(
                                    msg["validation_msg_isupdate"] % exsam["rack_tube"])
                        else:  # allow for update after approval in the same profile
                            self.warnings.append(msg["validation_msg_warning_update_submitted_sample"] % (
                                exsam["rack_tube"], exsam["biosampleAccession"]))
                        #    #rack_tube has already been approved by sample manager and can't be updated any more
                        #    self.errors.append(msg["validation_msg_duplicate_tube_or_well_id_in_copo"] % (err))
                        #    self.flag = False
                    else:
                        # rack_tube exist in another profile, can't be updated
                        self.errors.append(
                            msg["validation_msg_duplicate_tube_or_well_id_in_copo"] % exsam["rack_tube"])
                        self.flag = False

            # duplicates are allowed for asg (and possibily dtol) but one element of duplicate set must have one
            # and only one target in
            # sybiont fields
            for i in u:
                rack, tube = i.split('/')
                rows = self.data.loc[
                    (self.data.get("RACK_OR_PLATE_ID", "") == rack) & (self.data["TUBE_OR_WELL_ID"] == tube)]
                counts = Counter([x.upper()
                                  for x in list(rows["SYMBIONT"].values)])
                if "TARGET" not in counts:
                    self.errors.append(msg["validation_msg_duplicate_without_target"] % (
                        str(rows.get("RACK_OR_PLATE_ID", "") + "/" + rows["TUBE_OR_WELL_ID"])))
                    self.flag = False
                if counts["TARGET"] > 1:
                    self.errors.append(
                        msg["validation_msg_multiple_targets_with_same_id"] % (i))
                    self.flag = False
                # TODO this can go at version 2.3 of DTOL
                if counts["TARGET"] + counts["SYMBIONT"] < len(list(rows["SYMBIONT"].values)):
                    self.errors.append(
                        msg["validation_msg_multiple_targets_with_same_id"] % (i))
                    self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")


class DecimalLatitudeLongitudeValidator(Validator):
    def validate(self):
        """
            Check if sample has an ERGA manifest type and validate the fields - DECIMAL_LATITUDE, DECIMAL_LONGITUDE,
            LATITUDE_START, LATITUDE_END, LONGITUDE_START and LONGITUDE_END based on the following scenarios:

            1) If any of the aforementioned fields is empty, then the valdiation should fail
               because if there is no value, 'NOT_COLLECTED' should be the value

            2) All of the aforementioned fields cannot be 'NOT_COLLECTED'. The validation should fail if that occurs.

            3) If LATITUDE_START, LATITUDE_END, LONGITUDE_START and LONGITUDE_END have a decimal value then,
               DECIMAL_LATITUDE and DECIMAL_LONGITUDE must have the value 'NOT_COLLECTED'

            4) If DECIMAL_LATITUDE and DECIMAL_LONGITUDE have a decimal value then, LATITUDE_START, LATITUDE_END,
               LONGITUDE_START and LONGITUDE_END must have the value 'NOT_COLLECTED'
        """

        p_type = Profile().get_type(profile_id=self.profile_id)
        associated_p_type_lst = Profile().get_associated_type(
            self.profile_id, value=True, label=False)

        if "ERGA" in p_type:
            for index, row in self.data.iterrows():
                decimal_latlong_lst = [
                    row.get("DECIMAL_LATITUDE", ""), row.get("DECIMAL_LONGITUDE", "")]
                latlong_start_end_lst = [row.get("LATITUDE_START", ""), row.get("LATITUDE_END", ""),
                                         row.get("LONGITUDE_START", ""), row.get("LONGITUDE_END", "")]

                if all(i == "" for i in decimal_latlong_lst) and all(i == "" for i in latlong_start_end_lst):
                    self.errors.append(
                        msg["validation_msg_error_decimal_latlong_or_latlong_start_end_missing_value"] % str(
                            index + 2))
                    self.flag = False
                elif any(i == "" for i in decimal_latlong_lst) or any(i == "" for i in latlong_start_end_lst):
                    # erga manifests that inlude "POP_GENOMICS" as an associated tol project type
                    # are allowed to have blank field
                    if  any(i == "" for i in latlong_start_end_lst) \
                        and "POP_GENOMICS" in associated_p_type_lst and "BGE" in associated_p_type_lst:
                        pass
                    else:
                        self.errors.append(
                            msg["validation_msg_error_decimal_latlong_or_latlong_start_end_missing_value"] % str(
                                index + 2))
                        self.flag = False

                elif any(i == "NOT_COLLECTED" for i in decimal_latlong_lst) and any(
                    i != "NOT_COLLECTED" for i in decimal_latlong_lst) or any(
                        i == "NOT_COLLECTED" for i in latlong_start_end_lst) and any(
                        i != "NOT_COLLECTED" for i in latlong_start_end_lst):
                    self.errors.append(
                        msg["validation_msg_error_decimal_latlong_or_latlong_start_end_mixed_value"] % str(index + 2))
                    self.flag = False

                elif all(i == "NOT_COLLECTED" for i in decimal_latlong_lst) and all(
                        i == "NOT_COLLECTED" for i in latlong_start_end_lst):
                    self.errors.append(
                        msg["validation_msg_error_decimal_latlong_or_latlong_start_end_all_not_collected"] % str(
                            index + 2))
                    self.flag = False

                elif all(i == "NOT_COLLECTED" for i in decimal_latlong_lst) and any(
                        i == "NOT_COLLECTED" for i in latlong_start_end_lst):
                    self.errors.append(
                        msg["validation_msg_error_decimal_latlong_or_latlong_start_end_missing_start_end"] % str(
                            index + 2))
                    self.flag = False

                elif all(i == "NOT_COLLECTED" for i in latlong_start_end_lst) and any(
                        i == "NOT_COLLECTED" for i in decimal_latlong_lst):
                    self.errors.append(
                        msg["validation_msg_error_decimal_latlong_or_latlong_start_end_missing_decimal_latlong"] % str(
                            index + 2))
                    self.flag = False

                elif any(i == "NOT_COLLECTED" for i in decimal_latlong_lst) and any(
                        i == "NOT_COLLECTED" for i in latlong_start_end_lst):
                    self.errors.append(msg[
                        "validation_msg_error_decimal_latlong_or_latlong_start_end_not_collected_mixed_value"] % str(
                        index + 2))
                    self.flag = False

                elif all(i != "NOT_COLLECTED" for i in decimal_latlong_lst) and all(
                        i != "NOT_COLLECTED" for i in latlong_start_end_lst):
                    self.errors.append(
                        msg["validation_msg_error_decimal_latlong_or_latlong_start_end_all_contains_a_value"] % str(
                            index + 2))
                    self.flag = False

                else:
                    print('success')

        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")


class PopGenomicsAssociatedTypeValidator(Validator):
    def validate(self):
        """
            - Check if ERGA manifest is associated with Biodiversity Genomics Europe (BGE)
              and Population Genomics (POP_GENOMICS) tol project type
            - Perform validation if this is the case
            - Set default values for the optional fields if they are empty/blank i.e. not filled in by manifest submitter
        """
        p_name = Profile().get_name(self.profile_id)
        p_type = Profile().get_type(profile_id=self.profile_id)
        associated_p_type_lst = Profile().get_associated_type(
            self.profile_id, value=True, label=False)

        # Copy all values from the "PURPOSE_OF_SPECIMEN" column into a new
        # column called, "NEW_PURPOSE_OF_SPECIMEN"
        # self.data["NEW_PURPOSE_OF_SPECIMEN"] = self.data["PURPOSE_OF_SPECIMEN"]

        if "ERGA" in p_type:
            if "POP_GENOMICS" in associated_p_type_lst and "BGE" in associated_p_type_lst:
                if (self.data['PURPOSE_OF_SPECIMEN'] == 'RESEQUENCING').all():
                    # Associated tol project (s) for the manifest includes "POP_GENOMICS"
                    for index, row in self.data.iterrows():
                        # Update value of the created column, 'NEW_PURPOSE_OF_SPECIMEN' column to 'RESEQUENCING'
                        # because it is 'RESEQUENCING' for all rows
                        # self.data.at[index,
                        #              'NEW_PURPOSE_OF_SPECIMEN'] = 'RESEQUENCING'

                        # Set default value for the optional column if it was left blank
                        for optional_column in POP_GENOMICS_OPTIONAL_COLUMNS_DEFAULT_VALUES_MAPPING:
                            if not row.get(optional_column, "").strip():
                                if optional_column == 'GAL_SAMPLE_ID':
                                    default_value = self.data.at[index,
                                                                 "COLLECTOR_SAMPLE_ID"]
                                    self.data.at[index,
                                                 optional_column] = default_value
                                else:
                                    default_value = POP_GENOMICS_OPTIONAL_COLUMNS_DEFAULT_VALUES_MAPPING[
                                        optional_column]
                                    self.data.at[index,
                                                 optional_column] = default_value

                                # Display warning for fields where default values were set
                                self.warnings.append(msg["validation_msg_missing_optional_field_value"] % (
                                    optional_column, str(index + 2), default_value))
                else:
                    # Display error message for rows where value of 'PURPOSE_OF_SPECIMEN' column is not equal to
                    # 'RESEQUENCING'
                    rows_indices = self.data.index[
                        self.data.get("PURPOSE_OF_SPECIMEN", "") != 'RESEQUENCING'].tolist()

                    for index in rows_indices:
                        self.errors.append(
                            msg["validation_msg_invalid_purpose_of_specimen"]
                            % (self.data.at[index, 'PURPOSE_OF_SPECIMEN'], str(index + 2)))
                        self.flag = False
            elif ("POP_GENOMICS" not in associated_p_type_lst or  "BGE" not in associated_p_type_lst) and 'RESEQUENCING' in self.data[
                    'PURPOSE_OF_SPECIMEN'].unique():
                # Display error message if associated tol project(s) for the manifest does not include 
                # "POP_GENOMICS" and "BGE"
                p_name = f'{p_name[:25]}...' if len(
                    p_name) > 28 else p_name  # Truncate profile name to 25 characters
                self.errors.append(
                    msg["validation_msg_invalid_associated_tol_project"] % p_name)
                self.flag = False
            elif "POP_GENOMICS" in associated_p_type_lst:
                # Display error message if associated tol project(s) for the manifest includes "POP_GENOMICS"
                # but not "BGE"
                p_name = f'{p_name[:25]}...' if len(
                    p_name) > 28 else p_name
                self.errors.append(
                    msg["validation_msg_invalid_associated_tol_project"] % p_name)                
                self.flag = False
        return self.errors, self.warnings, self.flag, self.kwargs.get("isupdate")
