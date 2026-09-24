"""Sending channel-layer events from code that may or may not be on an event loop.

`async_to_sync` raises RuntimeError when called from a thread that already has
a running event loop. COPO's notification helpers are called from both plain
Celery tasks (no loop) and from code running under ASGI (loop present), so the
same call succeeds or explodes depending on the caller.

That is not theoretical: on 24 Sep a resubmission of ERR17892111 died at
register_project() -> update_submission_status() -> notify_status_change(),
after the project had been registered but before any run XML was sent, leaving
the submission half-done. One call site in common/utils/helpers.py already had
a local `try/except RuntimeError: pass` with the comment "sometimes causes a
RuntimeError"; the other seventeen did not.

A failed notification must never abort a submission: the UI update is
cosmetic, the ENA submission is not.

Kept free of imports with side effects so it can be unit tested on its own.
"""

import logging

logger = logging.getLogger(__name__)


def send_to_group(channel_layer, group_name, event, async_to_sync_fn=None):
    """Best-effort group_send. Returns True if the event was sent.

    Never raises: a notification failure must not take down the caller.
    """
    if channel_layer is None or not group_name:
        return False

    if async_to_sync_fn is None:  # pragma: no cover - real callers pass it in
        from asgiref.sync import async_to_sync as async_to_sync_fn

    try:
        async_to_sync_fn(channel_layer.group_send)(group_name, event)
        return True
    except RuntimeError as e:
        # "You cannot use AsyncToSync in the same thread as an async event loop"
        logger.debug(f"channel notification skipped for {group_name}: {e}")
        return False
    except Exception as e:  # noqa: BLE001 - notifications are never critical
        logger.warning(f"channel notification failed for {group_name}: {e}")
        return False
