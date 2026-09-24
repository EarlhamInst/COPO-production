"""Unit tests for best-effort channel-layer notification.

A resubmission of ERR17892111 died on 24 Sep at register_project() ->
update_submission_status() -> notify_status_change(), because async_to_sync
raises RuntimeError when the calling thread already has an event loop. The
project had been registered; no run XML was ever sent. A cosmetic UI update
must not be able to abort an ENA submission.
"""

from unittest import mock

import pytest

from common.utils.channels import send_to_group

GROUP = "submission_status_6a8579bc84a202f1f9a1ab34"
EVENT = {"type": "submission_status", "submission_id": "6aa40c4c42e0e1bebc53ca30"}


def _layer():
    layer = mock.MagicMock()
    layer.group_send = mock.MagicMock(name="group_send")
    return layer


def test_sends_the_event_and_reports_success():
    layer = _layer()
    sender = mock.MagicMock()

    assert send_to_group(layer, GROUP, EVENT, async_to_sync_fn=sender) is True

    sender.assert_called_once_with(layer.group_send)
    sender.return_value.assert_called_once_with(GROUP, EVENT)


def test_event_loop_conflict_is_swallowed():
    """The exact failure that aborted the resubmission."""
    def boom(_):
        raise RuntimeError(
            "You cannot use AsyncToSync in the same thread as an async event loop"
        )

    assert send_to_group(_layer(), GROUP, EVENT, async_to_sync_fn=boom) is False


def test_other_exceptions_are_swallowed_too():
    """A channel-layer outage must not abort a submission either."""
    def boom(_):
        raise ConnectionError("redis is unreachable")

    assert send_to_group(_layer(), GROUP, EVENT, async_to_sync_fn=boom) is False


@pytest.mark.parametrize(
    "layer, group",
    [
        (None, GROUP),
        (_layer(), ""),
        (None, ""),
    ],
)
def test_nothing_to_send_to_is_not_an_error(layer, group):
    sender = mock.MagicMock()

    assert send_to_group(layer, group, EVENT, async_to_sync_fn=sender) is False

    sender.assert_not_called()
