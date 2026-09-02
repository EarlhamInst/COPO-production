# Migrating production MinIO to single-node

**Status: PART-EXECUTED, PAUSED before cutover (2026-09-01).**

| Phase | State |
|---|---|
| 0 — Pre-flight | **Done.** IAM exported and copied off-host; sizing, volumes, nginx all verified. |
| 1 — New drives | **Done.** `minio-sn-data1/2` created on `ei-copo-prod-service`. |
| 2 — Mirror | **Done and converged.** 47/47 buckets, 105/105 objects. A second pass transferred 0 B. |
| 3 — Quiesce, final sync, verify | **Not started.** This is where the outage begins. |
| 4 — Cutover | **Not started.** |

**Live right now:** the old distributed cluster is still serving normally, and a
temporary `minio-sn` service is running alongside it on `ei-copo-prod-service`
holding a complete copy. Nothing destructive has happened; the old pool on
`minio-data{1,2}-{service,frontend}` is untouched.

**To resume:** re-run the Phase 2 convergence pass first (it is idempotent) to
pick up anything written since the pause, then continue at Phase 3.

**To abandon:** `docker service rm minio-sn` on `ei-copo-prod-sm`, and optionally
delete the `minio-sn-data*` volumes and their directories. The live cluster is
unaffected.

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

## Phase 0 — Pre-flight

1. **Fix SSH to the frontend node.** The 2026-08-27 capture failed with
   `Host key verification failed` on `ei-copo-prod-frontend`. You need both hosts.

2. **Size the data.** Use `mc admin info` — it reads the usage cache and returns
   instantly, where `mc du --recursive` walks every object.

   **Measured on prod 2026-09-01: 1.4 TiB used, 48 buckets, 105 objects**, one
   pool, one erasure set, stripe size 4, EC:2, 4 drives online / 0 offline.

   That is small enough that the mirror is a few hours, not days — and 105 objects
   is few enough to verify **exhaustively** (diff the full object listing old vs
   new and require an exact match) rather than by spot-check.

   This step is **informational, not a gate.**

   > **Ignore `df` on this mount.** It reports the export as *99% used with ~45T
   > available*, and that figure is **wrong** (confirmed 2026-09-01). There is no
   > SmartQuota on `copo_data/copo_live`, and OneFS reports capacity to NFS
   > clients in ways that do not correspond to usable free space. **Space is not a
   > constraint on this migration.** If you need a real number, get it from the
   > storage team or OneFS directly (`isi status`, or the WebUI) — not from `df`
   > on a COPO host.

   Ask storage admins for an **Isilon SnapshotIQ snapshot** of the MinIO
   directories before the window. Snapshots are copy-on-write and near-instant, so
   this is a point-in-time safety net at essentially no cost in space or time —
   far cheaper than any copy. Note that a same-cluster snapshot protects against
   procedural mistakes, not against the cluster failing; ask whether **SyncIQ**
   replicates `copo_live` off-cluster if you want cover for that.

3. **Confirm the NFS mount layout on `ei-copo-prod-service`:**
   ```bash
   docker volume inspect minio-data1 minio-data2 -f '{{.Name}} -> {{.Options}} {{.Mountpoint}}'
   df -h /mnt/copo-data/prod_copo
   ```

4. **Capture the IAM state — the step most likely to be forgotten.**

   **Confirmed on prod 2026-09-01:** there are **no IAM users** and **no custom
   policies** (only the five built-ins). The app authenticates as a **service
   account under the root user** — access key `dfdDKFJLKIerKJO`, `Policy: implied`,
   no expiry. Its secret is the Swarm secret `ecs_secret_key`.

   Service accounts live in `.minio.sys/` exactly like users do, and **`mc mirror`
   does not copy them.** Miss this and every app request returns `AccessDenied`
   after cutover, with the objects present and intact.

   Worse: a service account secret **cannot be read back** from MinIO — `svcacct
   info` returns the access key and never the secret. The only two ways to
   reconstitute it are the IAM export/import below, or recreating it explicitly
   from the values in the Swarm secrets `ecs_access_key` / `ecs_secret_key`.

   ```bash
   # on ei-copo-prod-service, as root; mc IS present in the MinIO image
   C=$(docker ps -qf name=copo_minio)
   docker exec "$C" sh -c 'mc alias set l http://127.0.0.1:9000 \
     "$(cat /run/secrets/minio_access_key)" "$(cat /run/secrets/minio_secret_key)"'
   docker exec "$C" mc admin user list l
   docker exec "$C" mc admin policy list l
   docker exec "$C" sh -c 'mc admin user svcacct list l "$(cat /run/secrets/minio_access_key)"'
   docker exec "$C" mc admin cluster iam export l     # writes /l-iam-info.zip (~1.7 KB)
   docker cp "$C":/l-iam-info.zip /root/l-iam-info.zip
   ```
   **Copy the zip off the host** — it is worthless if it only exists inside a
   container you are about to destroy.

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

**Volume facts, confirmed on prod 2026-09-01 — do not guess these:**

| | |
|---|---|
| Names | `minio-data1`, `minio-data2` — **no `copo_` prefix** (Swarm `external: true` volumes are not stack-prefixed) |
| Driver | **`local-persist`** (third-party plugin), *not* `local` |
| Mountpoints on `ei-copo-prod-service` | `/mnt/copo-data/prod_copo/minio-data1-service`, `.../minio-data2-service` |

The `-service` / `-frontend` suffix is per node: each host has its own directories
on the shared Isilon export.

`local-persist` takes a single `mountpoint` option — the `--opt type=none -o bind
-o device=` form used by the built-in `local` driver **does not work** with it.
On `ei-copo-prod-service` (as root; docker needs root on these hosts):

```bash
mkdir -p /mnt/copo-data/prod_copo/minio-sn-data1-service \
         /mnt/copo-data/prod_copo/minio-sn-data2-service

docker volume create -d local-persist \
  -o mountpoint=/mnt/copo-data/prod_copo/minio-sn-data1-service \
  --name=minio-sn-data1
docker volume create -d local-persist \
  -o mountpoint=/mnt/copo-data/prod_copo/minio-sn-data2-service \
  --name=minio-sn-data2
```

Match ownership/permissions to the existing `minio-data*-service` directories, or
MinIO will fail to format them.

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
  --mount type=volume,source=minio-sn-data1,target=/data1 \
  --mount type=volume,source=minio-sn-data2,target=/data2 \
  quay.io/minio/minio:RELEASE.2025-02-18T16-25-55Z-cpuv1 \
  server --console-address ":9001" /data1 /data2
```

**No `mc` sidecar is needed** — `mc` ships inside the MinIO image (confirmed
2026-09-01; note `which` is absent from that image, so probe with `mc --version`
rather than `which mc`). Drive everything from the running old-cluster container
on `ei-copo-prod-service`, as root:

```bash
C=$(docker ps -qf name=copo_minio)
docker exec "$C" sh -c 'mc alias set old http://127.0.0.1:9000 \
  "$(cat /run/secrets/minio_access_key)" "$(cat /run/secrets/minio_secret_key)"'
docker exec "$C" sh -c 'mc alias set new http://minio-sn:9000 \
  "$(cat /run/secrets/minio_access_key)" "$(cat /run/secrets/minio_secret_key)"'
```

Both aliases use the same root credentials, since `minio-sn` is created with the
same Swarm secrets. `old` goes via loopback to avoid depending on service DNS.

Mirror bucket by bucket. This is restartable and incremental — run it as many
times as you like while the old cluster serves traffic:

**Three gotchas, all hit for real on 2026-09-01:**

1. **One bucket has an invalid S3 name** — `682b7cb739ff65f8c6295d00_tmp`
   (underscore). MinIO accepted it historically, but `mc` rejects it client-side
   and **aborts any alias-level operation**, so `mc mirror old new` cannot be
   used. Mirror per bucket, filtering it out. Verified empty (40 KB of directory
   metadata per drive, no objects), so skipping it loses nothing.
2. **The MinIO image has no `awk`, `grep`, `which` or `head`** (UBI micro base).
   Run `mc` in the container but do all text processing **on the host**.
3. **`mc mirror` bucket-to-bucket does not create the target bucket** — it only
   does that alias-to-alias. Every target bucket needs an explicit
   `mc mb --ignore-existing` first, or every bucket fails with
   `The specified bucket does not exist`.

Build the bucket list on the host (expect 47 of the 48):

```bash
docker exec $(docker ps -qf name=copo_minio) mc ls old 2>/dev/null \
  | awk '{print $NF}' | tr -d '/' | grep -v '_' \
  | tee /root/buckets.txt | wc -l
```

Then run the mirror detached, so it survives an SSH drop:

```bash
nohup bash -c 'C=$(docker ps -qf name=copo_minio); while read -r b; do
    echo "=== $b ==="
    docker exec "$C" mc mb --ignore-existing "new/$b"
    docker exec "$C" mc mirror --preserve --retry "old/$b" "new/$b"
done < /root/buckets.txt' > /root/mirror.log 2>&1 &

tail -f /root/mirror.log
```

The whole loop is idempotent (`--ignore-existing` plus mirror's own comparison),
so re-run it as many times as you like — that is exactly how the Phase 3
incremental pass works. The old cluster serves normally throughout.

**Actual volume to copy: ~58 GiB**, not the 1.4 TiB that `mc admin info` reports
as used. The gap is almost certainly incomplete multipart uploads left by the
failed 500GB+ transfers — they consume space but are not objects, so they are not
mirrored. Worth reclaiming separately (`mc rm --incomplete --recursive`), but it
is not a blocker.

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
docker service rm minio-sn               # release the new volumes
docker service rm copo_minio               # stop the old distributed cluster
docker stack deploy -c deployment/copo.compose.production.yaml copo   # with your real tags
```

The new `minio` service claims `minio-sn-data1/2` — the drives already
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

The reverse proxy needs **no change** — verified 2026-09-01. `copo_nginx`
(`copo/copo-nginx-minio:v1.30.2`, running on `ei-copo-prod-frontend`) proxies to
the Swarm **service name**, not the per-slot hostnames, in
`/etc/nginx/conf.d/django_project.conf`:

```nginx
upstream minio          { server minio:9000; }   # minio.copo-project.org
upstream minio_console  { server minio:9001; }   # minio-console.copo-project.org
```

Since the migration keeps the service named `minio` and keeps
`--console-address ":9001"`, both upstreams resolve unchanged. No image rebuild
is required. (Its config is baked into the image, not this repo, so re-verify
with `docker exec $(docker ps -qf name=copo_nginx) grep -rn minio /etc/nginx/`
on `ei-copo-prod-frontend` if the image tag ever changes.)

---

## Rollback

Nothing destructive happens until Phase 4, and even then the old drives survive:

- **During Phases 1–3:** delete `minio-sn`. The old cluster never
  stopped. Zero impact.
- **After Phase 4:** `minio-data1/2` still hold the intact distributed pool.
  Revert the compose change (`git revert`), redeploy, and the old cluster
  reassembles. Any objects written *after* cutover exist only on the new drives,
  so mirror them back first.

Keep the old volumes until the cutover is signed off. Delete them only when you
are certain:

```bash
docker volume rm minio-data1 minio-data2   # on BOTH nodes, when happy
```

---

## If space ever does become a constraint

**It is not one today** (confirmed 2026-09-01: no quota, and the `df` figure is
erroneous — see Phase 0). Kept only in case that changes, or in case this
procedure is reused on a genuinely constrained filesystem.

1. **Reclaim first.** Drop MinIO's own versioning/incomplete-upload debris:
   `mc rm --incomplete --recursive`, and expire noncurrent versions if versioning
   is on. Large aborted multipart uploads from the failed 500GB+ transfers are a
   likely source of dead weight. Worth doing on its own merits before a long
   mirror — it is pure waste, and copying it wastes hours.
2. **Stage to a different filesystem** — mirror the old cluster to any other host
   or export with room, wipe the old drives, deploy single-node onto the *old*
   directories, then mirror back. Slower, and it gives up the intact-rollback
   property, so only do it if option 1 does not free enough.
3. **Do not proceed** with a wipe-then-restore on the same filesystem with no
   verified second copy. There is no safe version of that on production.
