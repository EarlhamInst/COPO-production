from .models import SequencingCentre, ProfileType, AssociatedProfileType

def get_all_sequencing_centres_for_options():
    scs = SequencingCentre.objects.all()
    return [{"value": s.name, "label": s.label} for s in scs]

def get_all_associated_profile_type_for_options():
    scs = AssociatedProfileType.objects.all()
    return [{"value": s.name, "label": s.label} for s in scs]


# return all profile types for which the user has permission or user is None
def get_all_profile_types_for_options_for_user(user=None):
    group_names = []
    if user:
        group_names  = user.groups.filter(name__regex=r'_users$').values_list('name', flat=True)
    pts = ProfileType.objects.all()
    return [{"value": p.type, "label": p.description} for p in pts if not p.is_permission_required or not user or f"{p.type}_users" in group_names]