# Migrating production MinIO to single-node

**Status:** procedure written 2026-09-01, **not yet executed**. The compose change
is committed (`deployment/copo.compose.production.yaml`); the data migration below
is the part that must be done by hand, in order.

> **This is a data migration, not a config change.** Do not deploy the new compose
> file on its own. Single-node MinIO cannot read the distributed pool's drives, so
> deploying without migrating objects first is an irreversible loss of every object
> in production. Read "Why you cannot just redeploy" before starting.

---

## Why you cannot just redeploy

The old config is a **4-drive erasure set spread over 2 nodes**
(`server http://minio-{1...2}/data{1...2}`). The new config is a **2-drive
single-node set** (`server /data1 /data2`) pinned to `ei-copo-prod-service`.

Three separate reasons the old data does not carry over:

1. **`format.json` is incompatible.** Each drive carries a `format.json` stamping
   its identity and the pool layout. A drive formatted for a distributed pool is
   rejected in single-node mode — MinIO refuses to start rather than adopt it.
   This is what killed the June demo attempt (`e4503bc6`, reverted 39 minutes
   later in `bb5cb91b`).
2. **Half the drives are dropped.** The old set has 4 drives; the new one has 2.
   The two drives on `ei-copo-prod-frontend` are not in the new pool at all.
3. **Objects are erasure-coded across all 4 drives**, so no single drive holds a
   complete copy of anything. You cannot recover objects by copying drive
   directories around — they must be read out through the S3 API while the old
   cluster is running.

**Therefore: the old cluster must be alive and serving to get the data out.**
Once you wipe or abandon its drives, the objects are gone.

---

## What this does and does not fix

**Fixes:** the quorum amplification. Prod is 2 nodes x 2 drives; write quorum
needs more than half, so losing either node drops 2 of 4 drives and writes stop.
There is currently *zero* single-node fault tolerance — any 20s heartbeat blip
between the two hosts is a write outage. That is the recurring incident. With one
node there is no peer to lose and no cross-node quorum to break.

**Does NOT fix:** the drives are on an **Isilon (Dell PowerScale)** NFS export
(`cyverse.ei.apricot.ciscloud:/CyVerse-iRODS/exports/copo_data/copo_live`), which
MinIO does not support and which is the prime suspect for the I/O stalls that
starve the grid heartbeat. Isilon **group changes** — node join/leave, drive
failure, FlexProtect/restripe jobs — pause NFS I/O for tens of seconds, matching
MinIO's 20s `last pong too old` threshold. That also fits a detail the network
theory never explained: the pongs failed in *both* directions, which a
host-to-host network fault rarely does but a simultaneous storage stall does.
Single-node makes such a stall a *slow request* instead of a *write outage*,
which is a large improvement, but the storage is still unsupported. Moving to
local block devices remains the correct long-term fix and is out of scope here.

**Trade-off accepted:** if `ei-copo-prod-service` goes down, MinIO is fully
offline (previously reads survived one node loss). This is a smaller loss than it
looks: all four drives already live on the *same* Isilon export, so the current
cross-node redundancy is largely fictitious — losing the export loses everything
either way.

**On "the data is scratch":** ENA is the system of record and COPO recreates
buckets on demand (`src/apps/copo_file/views.py:38-47`), so in principle these
drives could simply be wiped. **The decision (2026-09-01) is to migrate the data
anyway.** Two reasons it is worth the extra work: MinIO holds uploads that are
staged but not yet transferred to ENA — `validate_and_delete()`
(`common/s3/s3Connection.py:278`) only permits deletion once a transfer reaches
`DOWNLOADED_TO_LOCAL` — and those have no other copy, so wiping forces users to
re-upload, which is punishing at the 500GB+ file sizes involved. Migrating also
keeps the old pool intact as a real rollback. Do not "simplify" this procedure
back to a wipe without revisiting that decision.

---

## Phase 0 — Pre-flight (do this first, it can veto the plan)

1. **Fix SSH to the frontend node.** The 2026-08-27 capture failed with
   `Host key verification failed` on `ei-copo-prod-frontend`. You need both hosts.

2. **Size the data.** This decides whether the migration is even possible.
   ```bash
   mc alias set prodold http://minio:9000 "$ROOT_USER" "$ROOT_PASS"
   mc du --recursive prodold
   ```
   The export is **99% used with ~45T available**. The parallel-instance method
   below needs free space equal to the total object size. If `mc du` exceeds the
   headroom, stop and use the staged variant in "If there is not enough space".

   **Before assuming that 99% is a real ceiling:** on Isilon it is very often a
   **SmartQuota** on `copo_data/copo_live` rather than exhaustion of the 4.2P
   cluster. Ask storage admins whether the directory is under a quota, what the
   hard/soft limits are, and how much space the cluster actually has free. If the
   quota can be raised, the space problem disappears and no reclaim work is
   needed.

   Also ask for an **Isilon SnapshotIQ snapshot** of the MinIO directories before
   the window. Snapshots are copy-on-write and near-instant, so this is a
   point-in-time safety net at essentially no cost in space or time — far cheaper
   than any copy. Note a same-cluster snapshot protects against procedural
   mistakes, not against the cluster failing; ask whether **SyncIQ** replicates
   `copo_live` off-cluster if you want cover for that.

3. **Confirm the NFS mount layout on `ei-copo-prod-service`:**
   ```bash
   docker volume inspect copo_minio-data1 copo_minio-data2 -f '{{.Name}} -> {{.Options}} {{.Mountpoint}}'
   df -h /mnt/copo-data/prod_copo
   ```

4. **Capture the IAM state — the step most likely to be forgotten.**
   The app authenticates with `ecs_access_key` / `ecs_secret_key`, which are
   *different* Swarm secrets from `minio_access_key` / `minio_secret_key` (the
   MinIO root user). That strongly implies a MinIO IAM user stored inside
   `.minio.sys/`. **`mc mirror` copies objects only — it does not copy IAM users,
   policies, or bucket policies.** Miss this and every app request returns
   `AccessDenied` immediately after cutover, with the objects present and intact.
   ```bash
   mc admin user list prodold
   mc admin policy list prodold
   mc admin cluster iam export prodold        # writes a zip; keep it safe
   for b in $(mc ls prodold --json | jq -r .key); do
       echo "== $b"; mc anonymous get-json "prodold/$b" || true
   done > /tmp/bucket-policies.json
   ```
   Also record bucket-level settings that live outside object data: versioning,
   object locking, lifecycle rules, notifications.

5. **Record the live image tag.** Deploy drift is a known hazard here — the live
   stack has run tags ahead of what the repo pins. Use the tag you actually want
   live so you do not silently downgrade the app:
   ```bash
   docker service inspect copo_web -f '{{.Spec.TaskTemplate.ContainerSpec.Image}}'
   ```

6. **Announce a maintenance window.** Phase 3 quiesces writes. Uploads in flight
   will fail; ENA submissions in progress should be allowed to drain first.

---

## Phase 1 — Create the new drives (non-destructive)

The new single-node instance uses **new** directories, so the old distributed
drives stay untouched and remain the rollback. On `ei-copo-prod-service`:

```bash
sudo mkdir -p /mnt/copo-data/prod_copo/minio-sn-data1 \
              /mnt/copo-data/prod_copo/minio-sn-data2

docker volume create --driver local \
  --opt type=none --opt o=bind \
  --opt device=/mnt/copo-data/prod_copo/minio-sn-data1 copo_minio-sn-data1
docker volume create --driver local \
  --opt type=none --opt o=bind \
  --opt device=/mnt/copo-data/prod_copo/minio-sn-data2 copo_minio-sn-data2
```

Match ownership/permissions to the existing `minio-data*` directories, or MinIO
will fail to format them.

---

## Phase 2 — Stand up the new instance alongside the old, and mirror

The old cluster keeps serving throughout this phase. Create a **temporary**
service (same root secrets, so credentials do not change at cutover):

```bash
docker service create --name minio-sn \
  --hostname minio-sn \
  --network copo_backend \
  --endpoint-mode dnsrr \
  --constraint 'node.hostname==ei-copo-prod-service' \
  --secret minio_access_key --secret minio_secret_key \
  --env MINIO_ROOT_USER_FILE=/run/secrets/minio_access_key \
  --env MINIO_ROOT_PASSWORD_FILE=/run/secrets/minio_secret_key \
  --mount type=volume,source=copo_minio-sn-data1,target=/data1 \
  --mount type=volume,source=copo_minio-sn-data2,target=/data2 \
  quay.io/minio/minio:RELEASE.2025-02-18T16-25-55Z-cpuv1 \
  server --console-address ":9001" /data1 /data2
```

`copo_backend` is a Swarm overlay and is likely **not** `--attachable`, so plain
`docker run` will not reach it. Run `mc` as a service on the same network:

```bash
docker service create --name mctool \
  --network copo_backend \
  --constraint 'node.hostname==ei-copo-prod-service' \
  --entrypoint sleep minio/mc infinity

# then, on ei-copo-prod-service (docker exec is node-local):
C=$(docker ps -qf name=mctool)
docker exec $C mc alias set old http://minio:9000    "$ROOT_USER" "$ROOT_PASS"
docker exec $C mc alias set new http://minio-sn:9000 "$ROOT_USER" "$ROOT_PASS"
```

Mirror bucket by bucket. This is restartable and incremental — run it as many
times as you like while the old cluster serves traffic:

```bash
for b in $(docker exec $C mc ls old --json | jq -r .key | tr -d /); do
    docker exec $C mc mb --ignore-existing "new/$b"
    docker exec $C mc mirror --preserve --retry "old/$b" "new/$b"
done
```

Run it repeatedly until a pass copies almost nothing. Expect this to take a long
time over NFS — start it early.

---

## Phase 3 — Quiesce, final sync, verify

1. **Stop writes.** Scale the app down so nothing is uploading:
   ```bash
   docker service scale copo_web=0 copo_celery_worker=0 copo_celery_beat=0
   ```
   (Use the real service names from `docker service ls`.)

2. **Final incremental mirror** — same loop as Phase 2, plus `--remove` to catch
   deletions that happened during the long first pass:
   ```bash
   docker exec $C mc mirror --preserve --retry --remove "old/$b" "new/$b"
   ```

3. **Verify before you destroy anything.** Compare per-bucket object counts and
   total size on both sides; they must match exactly:
   ```bash
   docker exec $C mc du --recursive old
   docker exec $C mc du --recursive new
   ```
   Spot-check a few large objects with `mc stat` on both sides and compare ETags.
   **Do not proceed until these agree.**

4. **Restore IAM** from Phase 0, into the new instance:
   ```bash
   docker exec $C mc admin cluster iam import new /path/to/iam-export.zip
   docker exec $C mc admin user list new     # confirm the ecs_access_key user exists
   ```
   Re-apply any bucket policies recorded in Phase 0.

---

## Phase 4 — Cut over

```bash
docker service rm minio-sn mctool          # release the new volumes
docker service rm copo_minio               # stop the old distributed cluster
docker stack deploy -c deployment/copo.compose.production.yaml copo   # with your real tags
```

The new `minio` service claims `copo_minio-sn-data1/2` — the drives already
holding the mirrored data — and comes up single-node with the same root
credentials.

**No app config change is needed.** `ECS_ENDPOINT=http://minio:9000` and
`ECS_ENDPOINT_EXTERNAL=https://minio.copo-project.org` are unchanged; the service
name is still `minio`, so DNS resolves exactly as before.

Bring the app back:
```bash
docker service scale copo_web=1 copo_celery_worker=1 copo_celery_beat=1
```

### Verify

```bash
curl -s -o /dev/null -w "%{http_code}\n" https://minio.copo-project.org/minio/health/live   # 200
curl -s -o /dev/null -w "%{http_code}\n" https://minio.copo-project.org                     # 403 = healthy
docker service logs copo_minio --tail 50 --no-trunc
```

`403` on the root is correct (unauthenticated S3). Then exercise the real path:
upload a file through COPO and download it again. Confirm the logs contain **no**
`last pong too old` and no `InsufficientWriteQuorum` — with one node those errors
are now structurally impossible, so their absence is the signal the fix landed.

Also confirm the reverse proxy still resolves its upstream: `copo/copo-nginx-minio`
fronts `minio.copo-project.org` and its config is baked into the image, not in
this repo. If it targets `minio-1`/`minio-2` rather than `minio`, it will need a
rebuild — check before the window, not during it.

---

## Rollback

Nothing destructive happens until Phase 4, and even then the old drives survive:

- **During Phases 1–3:** delete `minio-sn` and `mctool`. The old cluster never
  stopped. Zero impact.
- **After Phase 4:** `copo_minio-data1/2` still hold the intact distributed pool.
  Revert the compose change (`git revert`), redeploy, and the old cluster
  reassembles. Any objects written *after* cutover exist only on the new drives,
  so mirror them back first.

Keep the old volumes until the cutover is signed off. Delete them only when you
are certain:

```bash
docker volume rm copo_minio-data1 copo_minio-data2   # on BOTH nodes, when happy
```

---

## If there is not enough space

If `mc du` exceeds the free space on the export, the parallel-instance method
will not fit. Options, in order of preference:

1. **Reclaim first.** Drop MinIO's own versioning/incomplete-upload debris:
   `mc rm --incomplete --recursive`, and expire noncurrent versions if versioning
   is on. Large aborted multipart uploads from the failed 500GB+ transfers are a
   likely source of dead weight.
2. **Stage to a different filesystem** — mirror the old cluster to any other host
   or export with room, wipe the old drives, deploy single-node onto the *old*
   directories, then mirror back. This is slower and gives up the intact-rollback
   property, so only do it if option 1 does not free enough.
3. **Do not proceed** with a wipe-then-restore on the same filesystem with no
   verified second copy. There is no safe version of that on production.
