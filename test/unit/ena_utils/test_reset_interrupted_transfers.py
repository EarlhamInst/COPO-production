"""Unit tests for restarting file transfers interrupted by a worker stopping.

A transfer that a stopped worker left as 'processing' can't still be running
once a 'file_transfers' worker starts so, it is put back to 'pending'.
Otherwise, it waits up to 12 hours for `check_for_stuck_transfers`, and no
other upload starts while two uploads are 'processing'.
"""

from types import SimpleNamespace
from unittest import mock

from common.ena_utils import FileTransferUtils
from src import celery as copo_celery


def worker(*queue_names):
    # The `worker_ready` signal's sender is the worker's consumer
    return SimpleNamespace(
        task_consumer=SimpleNamespace(queues=[SimpleNamespace(name=name) for name in queue_names])
    )


def test_processing_transfers_are_put_back_to_pending():
    transfers = mock.patch.object(FileTransferUtils, 'EnaFileTransfer')
    with transfers as ena_file_transfer:
        ena_file_transfer.return_value.get_collection_handle.return_value.find.return_value = [
            {'_id': 'tx1', 'local_path': 'read_test3.fastq.gz'},
            {'_id': 'tx2', 'local_path': 'read_test4.fastq.gz'},
        ]
        FileTransferUtils.reset_interrupted_transfers()

    ena_file_transfer.return_value.get_collection_handle.return_value.find.assert_called_once_with(
        {'status': 'processing'}, {'local_path': 1}
    )
    assert ena_file_transfer.return_value.set_pending.call_args_list == [
        mock.call('tx1'), mock.call('tx2')
    ]


def test_file_transfers_worker_resets_interrupted_transfers_on_start():
    with mock.patch.object(FileTransferUtils, 'reset_interrupted_transfers') as reset:
        copo_celery.reset_interrupted_file_transfers(sender=worker('file_transfers'))

    reset.assert_called_once()


def test_other_workers_leave_transfers_alone():
    with mock.patch.object(FileTransferUtils, 'reset_interrupted_transfers') as reset:
        copo_celery.reset_interrupted_file_transfers(sender=worker('celery'))

    reset.assert_not_called()
