"""Pluggable LIMS adapters for the EDP flow.

Public surface:

    from src.apps.ei_edp.utils.lims import get_lims_adapter, LIMSAdapter

`get_lims_adapter()` returns the adapter named by the `LIMS_ADAPTER` Django
setting (a dotted path to a `LIMSAdapter` subclass), defaulting to the no-op
`NullLIMSAdapter` when the setting is unset or empty. For example:

    LIMS_ADAPTER = "src.apps.ei_edp.utils.lims.sapio.adapter.SapioAdapter"

Any facility can point this at their own adapter class without touching COPO.
"""

from importlib import import_module

from django.conf import settings

from .base import LIMSAdapter, LIMSUnavailable, NullLIMSAdapter

__all__ = ["get_lims_adapter", "LIMSAdapter", "LIMSUnavailable", "NullLIMSAdapter"]

# Cache the resolved instance; the setting is fixed for the process lifetime.
_adapter_instance: LIMSAdapter = None


def _resolve_adapter_class(dotted_path: str):
    module_path, _, class_name = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(
            f"LIMS_ADAPTER must be a full dotted path to a class, got: {dotted_path!r}"
        )
    module = import_module(module_path)
    return getattr(module, class_name)


def get_lims_adapter() -> LIMSAdapter:
    """Return the process-wide LIMS adapter instance (cached)."""
    global _adapter_instance
    if _adapter_instance is not None:
        return _adapter_instance

    dotted_path = getattr(settings, "LIMS_ADAPTER", "") or ""
    if not dotted_path:
        _adapter_instance = NullLIMSAdapter()
        return _adapter_instance

    adapter_class = _resolve_adapter_class(dotted_path)
    if not issubclass(adapter_class, LIMSAdapter):
        raise TypeError(
            f"LIMS_ADAPTER {dotted_path!r} is not a subclass of LIMSAdapter"
        )
    _adapter_instance = adapter_class()
    return _adapter_instance
