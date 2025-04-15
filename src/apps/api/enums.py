from enum import Enum
from src.apps.copo_core.models import ProfileType, AssociatedProfileType
from django.conf import settings
from common.dal.sample_da import Sample
from itertools import chain


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
