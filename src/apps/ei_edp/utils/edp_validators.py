from common.dal.sample_da import Sample
from common.utils import helpers
from common.validators.validator import Validator

from .edp_utils import get_samples_left_to_upload
from .edp_validation_messages import MESSAGES

# Label of the sample manifest column that holds each sample's name
SAMPLE_NAME_LABEL = 'Sample'


class SampleCountValidator(Validator):
    '''Under EDP profiles, a sample manifest cannot add more samples than
    are left to upload i.e. the number of samples requested on the profile
    less those that have already been uploaded.

    Rows whose sample name already exists in the profile update that
    sample so, only rows with new sample names are counted.
    '''

    def validate(self):
        checklist = self.kwargs.get('checklist', {})
        sample_name_column = next(
            (
                key
                for key, field in checklist.get('fields', {}).items()
                if field.get('label') == SAMPLE_NAME_LABEL
            ),
            None,
        )
        if not sample_name_column or sample_name_column not in self.data.columns:
            # A missing sample name column is reported by the mandatory values check
            return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')

        # Blank cells are read in as 'nan' since every value is converted to a string
        sample_names = {
            name for name in self.data[sample_name_column] if name and name != 'nan'
        }
        existing_sample_names = set(
            Sample()
            .get_collection_handle()
            .distinct(
                'name',
                {
                    'profile_id': self.profile_id,
                    'deleted': helpers.get_not_deleted_flag(),
                    'name': {'$in': list(sample_names)},
                },
            )
        )
        new_sample_count = len(sample_names - existing_sample_names)

        profile = self.kwargs.get('profile') or {}
        samples_left_to_upload = get_samples_left_to_upload(profile)
        if new_sample_count > samples_left_to_upload:
            self.errors.append(
                MESSAGES['too_many_samples'].format(
                    new_sample_count=new_sample_count,
                    samples_left_to_upload=samples_left_to_upload,
                )
            )
            self.flag = False

        return self.errors, self.warnings, self.flag, self.kwargs.get('isupdate')
