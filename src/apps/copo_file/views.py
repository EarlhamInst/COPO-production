from django.contrib.auth.decorators import login_required
from common.dal.profile_da import Profile
from common.s3.s3Connection import S3Connection
from django.shortcuts import render
import jsonpickle
from django.http import HttpResponse
from .utils.CopoFiles import generate_files_record
from common.utils import helpers
    chunk_size = 64 * MB
    transfer_config = TransferConfig(
        multipart_threshold=chunk_size,
        multipart_chunksize=chunk_size,
        max_concurrency=10,
        use_threads=True,
    )

    total_files = len(files)
    for file_idx, f in enumerate(files, start=1):
        file = files[f]
        key = file.name.replace(" ", "-")

        total_chunks = max(1, -(-file.size // chunk_size))  # ceiling division

        helpers.notify_frontend(
            data={"profile_id": profile_id},
            msg=json.dumps({"file_name": key, "file_num": file_idx,
                            "total_files": total_files, "chunk": 0,
                            "total_chunks": total_chunks}),
            action="upload_progress",
            html_id="upload_progress", group_name=channels_group_name)

        progress_state = {"bytes": 0, "last_pct": -1}
        progress_lock = threading.Lock()

        def _progress(bytes_amount, _key=key, _file_idx=file_idx,
                      _total_chunks=total_chunks, _size=file.size):
            with progress_lock:
                progress_state["bytes"] += bytes_amount
                pct = int(progress_state["bytes"] * 100 / _size) if _size else 100
                # Throttle: only notify when integer percent advances by >=2
                if pct - progress_state["last_pct"] < 2 and pct < 100:
                    return
                progress_state["last_pct"] = pct
                chunk = min(_total_chunks, max(1, (pct * _total_chunks) // 100))
            helpers.notify_frontend(
                data={"profile_id": profile_id},
                msg=json.dumps({"file_name": _key, "file_num": _file_idx,
                                "total_files": total_files, "chunk": chunk,
                                "total_chunks": _total_chunks}),
                action="upload_progress",
                html_id="upload_progress", group_name=channels_group_name)

        s3.s3_client.upload_fileobj(
            Fileobj=file,
            Bucket=bucket_name,
            Key=key,
            Config=transfer_config,
            Callback=_progress,
        )

    context = dict()
    context["table_data"] = generate_files_record(profile_id=profile_id)
    context["component"] = "files"
    out = jsonpickle.encode(context, unpicklable=False)
    return HttpResponse(status=200, content=out, content_type='application/json')
