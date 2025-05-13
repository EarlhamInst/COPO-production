from enum import Enum
from django.conf import settings

import common.schemas.utils.data_utils as d_utils
from common.dal.sample_da import Sample
from src.apps.copo_core.models import (
    ProfileType,
    AssociatedProfileType,
    SequencingCentre,
)


class AssociatedProjectEnum(str, Enum):
    # ERGA associated profile types excluding SANGER
    @classmethod
    def values(cls):
        return list(
            AssociatedProfileType.objects.filter(profiletype__type='erga')
            .exclude(name='SANGER')
            .order_by('name')
            .values_list('name', flat=True)
            .distinct()
        )


class ProjectEnum(str, Enum):
    @classmethod
    def values(cls):
        return list(
            ProfileType.objects.filter(is_dtol_profile=True)
            .order_by('type')
            .values_list('type', flat=True)
            .distinct()
        )


class ProjectVersionsEnum(str, Enum):
    @classmethod
    def values(cls):
        schema = Sample().get_component_schema()
        project_names = {p.lower() for p in ProjectEnum.values()}
        manifest_versions = sorted(
            {
                mv
                for x in schema
                if project_names.intersection(
                    map(str.lower, x.get('specifications', []))
                )
                for mv in x.get('manifest_version', [])
            },
            reverse=True,
        )
        return manifest_versions


class ReturnTypeEnum(str, Enum):
    JSON = 'json'
    CSV = 'csv'

    @classmethod
    def values(cls):
        return [e.value for e in cls]


class SequencingCentreEnum(str, Enum):
    @classmethod
    def values(cls):
        return list(
            SequencingCentre.objects.order_by('label')
            .values_list('label', flat=True)
            .distinct()
        )


class SampleFieldsEnum(str, Enum):
    @classmethod
    def values(cls):
        # NB: 'TAXON_ID' is excluded because there is another API endpoint for it
        # NB: 'copo_audit_type' is excluded because it is not a sample field
        excluded_fields = ['TAXON_ID', 'copo_audit_type']
        fields = []
        seen = set()

        projects = ProjectEnum.values()
        if 'dtolenv' in projects:
            projects.remove('dtolenv')  # Exclude 'dtolenv' from the list of projects

        for project in projects:
            manifest_version = settings.MANIFEST_VERSION.get(project.upper())
            if not manifest_version:
                continue

            sample_fields = d_utils.get_export_fields(
                component='sample', project=project, manifest_version=manifest_version
            )
            for field in sample_fields:
                if field not in seen:
                    fields.append(field)
                    seen.add(field)

        # Remove excluded fields
        fields = [f for f in fields if f not in excluded_fields]
        return sorted(fields)


class StandardEnum(str, Enum):
    @classmethod
    def values(cls):
        schema = Sample().get_component_schema()
        project_names = {p.lower() for p in ProjectEnum.values()}
        standards = sorted(
            {
                std
                for x in schema
                if project_names.intersection(
                    map(str.lower, x.get('specifications', []))
                )
                for std in x.get('standards', {})
            },
        )
        return standards


class UpdateAuditFieldEnum(str, Enum):
    RACK_OR_PLATE_ID = "RACK_OR_PLATE_ID"
    SPECIMEN_ID = "SPECIMEN_ID"
    TUBE_OR_WELL_ID = "TUBE_OR_WELL_ID"
    BIOSAMPLE_ACCESSION = "biosampleAccession"
    PUBLIC_NAME = "public_name"
    SRA_ACCESSION = "sraAccession"

    @classmethod
    def values(cls):
        return [e.value for e in cls]


class UpdateTypeEnum(str, Enum):
    SYSTEM = 'system'
    USER = 'user'

    @classmethod
    def values(cls):
        return [e.value for e in cls]
