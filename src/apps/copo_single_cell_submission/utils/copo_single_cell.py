from common.utils.logger import Logger
from django.contrib.auth.models import User
from .da import SinglecellSchemas

l = Logger()


def generate_singlecell_record(profile_id, checklist_id=str()):
    checklist = SinglecellSchemas().get_schema(target_id=checklist_id)

    if not checklist:
        return dict(dataSet=[],
                    columns=[],
                    )

    data_set = []
    columns = []

    return_dict = dict(dataSet=data_set,
                       columns=columns,
                       #bucket_size_in_GB=round(bucket_size/1024/1024/1024,2),  
                       )

    return return_dict