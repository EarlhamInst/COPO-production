# Moving production MinIO to single-node

**Status:** written 2026-09-01, **not yet executed**. The compose change is on
this branch; the steps below are done by hand, in order.

**Premise: the objects in prod MinIO are scratch data.** ENA is the system of
record, and COPO recreates buckets on demand, so the drives can simply be wiped.
If that ever stops being true, stop and read "If the data stops being scratch".

Expect ~30 minutes including verification.

---

## Why this fixes the recurring outage

Prod is a **4-drive erasure set across 2 nodes**
(`server http://minio-{1...2}/data{1...2}`). Write quorum needs more than half
the drives, so losing either node drops 2 of 4 and writes stop outright — there
is **zero single-node fault tolerance**. Any ~20s stall between the hosts is a
write outage. That is the incident that has recurred through 2026-08-27:
`last pong too old (20s); disconnecting` followed by `InsufficientWriteQuorum`.

Single-node (`server /data1 /data2`, pinned to one host) has no peer to lose and
no cross-node quorum to break, so that failure mode becomes structurally
impossible.

**It does not fix the underlying cause.** The drives are on an Isilon
(PowerScale) NFS export, which MinIO does not support. Isilon group changes —
node join/leave, drive failure, FlexProtect/restripe — pause NFS I/O for tens of
seconds, matching the 20s heartbeat threshold. Single-node turns such a stall
into a *slow request* instead of a *write outage*, which is the win here. Moving
MinIO to local block devices remains the correct long-term fix.

**Trade-off:** if `ei-copo-prod-service` goes down, MinIO is fully offline
(previously reads survived one node loss). Small in practice — all four drives
already sit on the same Isilon export, so the current cross-node redundancy is
largely fictitious.

---

## The two things that actually bite

Everything else here is routine. These two are not:

### 1. IAM does not survive the wipe

The app authenticates with `ecs_access_key` / `ecs_secret_key`, which are
**different Swarm secrets** from `minio_access_key` / `minio_secret_key` (the
MinIO root user). That means there is almost certainly a MinIO IAM user stored in
`.minio.sys/` — **on the drives you are about to wipe.**

Miss this and COPO gets `AccessDenied` on every request after cutover. It cannot
even auto-create buckets. The objects being gone is expected and fine; the app
being unable to authenticate is a real outage.

**Export before wiping, on `ei-copo-prod-service`:**
```bash
C=$(docker ps -qf name=copo_minio)
docker exec $C sh -c 'mc alias set l http://127.0.0.1:9000 \
  "$(cat /run/secrets/minio_access_key)" "$(cat /run/secrets/minio_secret_key)"'
docker exec $C mc admin user list l
docker exec $C mc admin policy list l
docker exec $C mc admin cluster iam export l     # keep the zip off this host
```
If `mc` is absent from the image, run it as a throwaway service on the same
overlay:
```bash
docker service create --name mctool --network copo_backend \
  --constraint 'node.hostname==ei-copo-prod-service' \
  --entrypoint sleep minio/mc infinity
```

If `mc admin user list` shows **no** users, then `ecs_access_key` is just the
root key under another name and there is nothing to restore — confirm that before
relying on it.

### 2. Staged-but-unsubmitted uploads are lost

`validate_and_delete()` (`common/s3/s3Connection.py:278`) only lets users delete a
file once its ENA transfer has reached `DOWNLOADED_TO_LOCAL`. So MinIO holds
uploads that have **not yet reached ENA**, and those have no other copy. Users
will have to re-upload them — which matters, given the 500GB+ transfers involved.

Warn users before the window, and let in-flight ENA submissions drain first.

---

## Procedure

### 1. Pre-flight

On `ei-copo-prod-service` — capture IAM (section 1 above), then record the live
image tags so the redeploy does not silently downgrade the app. Deploy drift is a
known hazard here. On the Swarm manager (`ei-copo-prod-sm`):
```bash
for s in copo_web copo_minio copo_nginx; do
    echo -n "$s: "; docker service inspect $s -f '{{.Spec.TaskTemplate.ContainerSpec.Image}}'
done
```

Check the reverse proxy: `copo/copo-nginx-minio` fronts `minio.copo-project.org`
and its config is baked into the image, not in this repo. If its upstream targets
`minio-1`/`minio-2` rather than `minio`, it needs a rebuild — find out now, not
during the window.

Announce the window.

### 2. Stop the app and the old cluster

On `ei-copo-prod-sm`:
```bash
docker service scale copo_web=0 copo_celery_worker=0 copo_celery_beat=0
docker service rm copo_minio          # also detaches the volumes
```
(Use the real service names from `docker service ls`.)

### 3. Wipe the drives

MinIO will not start on drives carrying a distributed pool's `format.json`. This
is what killed the June demo attempt (`e4503bc6`, reverted 39 minutes later in
`bb5cb91b`) — it was not wiped.

Run on **both** nodes, since each holds its own node-local copies:
```bash
# on ei-copo-prod-service AND ei-copo-prod-frontend
docker volume rm copo_minio-data1 copo_minio-data2
```
Then recreate them on `ei-copo-prod-service` only, pointing at the same Isilon
paths, matching the ownership/permissions the old directories had:
```bash
docker volume create --driver local --opt type=none --opt o=bind \
  --opt device=/mnt/copo-data/prod_copo/minio-data1 copo_minio-data1
docker volume create --driver local --opt type=none --opt o=bind \
  --opt device=/mnt/copo-data/prod_copo/minio-data2 copo_minio-data2
```
Clear any leftover contents in those directories, including `.minio.sys`.

### 4. Deploy and restore IAM

On `ei-copo-prod-sm`:
```bash
docker stack deploy -c deployment/copo.compose.production.yaml copo   # with your real tags
```
Then restore identity, on `ei-copo-prod-service`:
```bash
docker exec $C mc admin cluster iam import l /path/to/iam-export.zip
docker exec $C mc admin user list l          # the ecs_access_key user must be back
```

Bring the app back:
```bash
docker service scale copo_web=1 copo_celery_worker=1 copo_celery_beat=1
```

**No app config change is needed.** `ECS_ENDPOINT=http://minio:9000` and
`ECS_ENDPOINT_EXTERNAL=https://minio.copo-project.org` are unchanged — the
service is still named `minio`, so DNS resolves exactly as before.

### 5. Verify

```bash
curl -s -o /dev/null -w "%{http_code}\n" https://minio.copo-project.org/minio/health/live   # 200
curl -s -o /dev/null -w "%{http_code}\n" https://minio.copo-project.org                     # 403 = healthy
docker service logs copo_minio --tail 50 --no-trunc
```
`403` on the root is correct (unauthenticated S3). Then exercise the real path:
upload a file through COPO and download it again — this also confirms bucket
auto-creation works with the restored IAM user.

Confirm the logs show **no** `last pong too old` and no `InsufficientWriteQuorum`.
With one node those are now structurally impossible, so their absence is the
signal the fix landed.

Delete `mctool` if you created it.

---

## Rollback

Revert the compose change (`git revert`), wipe the drives again (single-node
`format.json` is equally unreadable to a distributed pool), and redeploy. Objects
are lost either way — they were scratch. Restore IAM from the same export.

Rolling back returns you to a topology with zero fault tolerance, so only do it
if single-node itself proves worse.

---

## If the data stops being scratch

This procedure is safe **only** while ENA remains the system of record and the
objects are disposable. If prod MinIO ever holds data that is not reproducible,
none of the above applies: single-node cannot read the distributed pool's drives,
objects are erasure-coded across all four so drive directories cannot be copied,
and the data must be read out through the S3 API while the old cluster is still
running.

In that case the procedure becomes: stand up a second MinIO on new drive
directories, `mc mirror` bucket by bucket while the old cluster still serves,
quiesce, do a final `mc mirror --remove` pass, verify object counts and sizes
match, then cut over — keeping the old volumes intact as the rollback. That needs
free space equal to the total object size; check `mc du --recursive` first. On
Isilon, a SnapshotIQ snapshot is a far cheaper pre-cutover safety net than any
copy, and the "99% used" figure on the export may be a SmartQuota that storage
admins can raise rather than a real ceiling.
