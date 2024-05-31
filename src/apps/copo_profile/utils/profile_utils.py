from src.apps.copo_core.models import SequencingCentre

def get_all_sequencing_centres():
    scs = SequencingCentre.objects.all()
    return [{"value": s.name, "label": s.label} for s in scs]