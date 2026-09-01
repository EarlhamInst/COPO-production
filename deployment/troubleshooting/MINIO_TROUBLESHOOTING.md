# MinIO Troubleshooting (demo / dev)

**Symptom:** the app can't connect to object storage; uploads/downloads fail;
`http://minio.copodev.cyverseuk.org` returns **503**; COPO surfaces it as an S3
connection error.

This has recurred multiple times on the demo/dev swarm (`ei-copo-demo-*`). The
**root cause is the distributed MinIO design**, and the **recommended durable fix
is to run demo/dev MinIO single-node**. Don't go down the firewall rabbit hole
first — see below.

> **Deployment status:** single-node is **NOT currently deployed**. Demo/dev (and
> production) still run the **distributed** MinIO config in `copo.compose.yaml` /
> `copo.compose.production.yaml`. The single-node setup below is a documented,
> tested fix to **apply when this recurs** — it has not been committed to the
> compose files. Treat the "TL;DR fix" as a change to make, not the current state.

---

## TL;DR fix

Reconfigure demo MinIO to single-node, pinned to one node, and wipe the old
volumes (their distributed `format.json` is incompatible with single-node).

In `deployment/copo.compose.yaml`:

```yaml
# x-minio-common anchor — command:
command: server --console-address ":9001" /data1 /data2   # NOT http://minio-{1...2}/data{1...2}

# minio service:
minio:
  <<: *minio-common
  hostname: minio                                          # NOT minio-{{.Task.Slot}}
  volumes:
    - minio-data1:/data1
    - minio-data2:/data2
  deploy:
    replicas: 1                                            # NOT 2
    endpoint_mode: dnsrr
    placement:
      constraints:
        - "node.hostname==ei-copo-demo-service"            # pin to ONE node
    restart_policy:
      condition: any
```

Deploy:

```bash
docker service rm copo_minio                               # detach volumes
# ON ei-copo-demo-service:
docker volume rm copo_minio-data1 copo_minio-data2         # wipe old distributed format (loses demo objects)
docker stack deploy -c deployment/copo.compose.yaml copo   # use the host's actual deploy file/tags
```

Verify:

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://minio.copodev.cyverseuk.org/minio/health/live   # 200 = healthy
curl -s -o /dev/null -w "%{http_code}\n" http://minio.copodev.cyverseuk.org                      # 403 = healthy (unauth S3, correct)
```

`200` on `health/live` and `403` on root = working. `503` = still broken.

---

## Why this happens (root cause)

Demo/dev MinIO was deployed **distributed**:
`server ... http://minio-{1...2}/data{1...2}`, `hostname: minio-{{.Task.Slot}}`,
`replicas: 2`, with **node-local** named volumes `minio-data1/2`.

Two independent fragilities, both triggered by a redeploy/reschedule:

1. **Overlay network state corruption.** When a MinIO task thrashes across nodes
   during a deploy, the `copo_backend` overlay's VTEP/FDB entries go stale. Peers
   then fail to connect (`grid ... i/o timeout`, `drive not found`). This state
   lives in the **swarm network DB (manager raft)**, so a daemon restart does
   **not** clear it.

2. **Drive-ordering mismatch.** Slots are assigned by Swarm
   (`hostname: minio-{{.Task.Slot}}`) but volumes are node-local with identity
   stamped in each drive's `format.json`. If a reschedule puts slot 1 on a
   different node than its volumes, MinIO reports
   `unexpected drive ordering on pool ... expected at (drive=Nth)` and refuses to
   assemble the pool (`Waiting for a minimum of 2 drives to come online`).

Single-node removes peers, quorum, and cross-node drive ordering entirely — none
of the above can happen.

---

## Diagnosis chain (if you want to confirm before fixing)

1. **Health check** — fast 503 (not a timeout) means the host is reachable but
   MinIO has no quorum:
   ```bash
   curl -s -o /dev/null -w "%{http_code} connect=%{time_connect}s\n" http://minio.copodev.cyverseuk.org
   ```

2. **MinIO logs** — what's the actual error?
   ```bash
   docker service logs copo_minio --tail 60 --no-trunc
   ```
   - `i/o timeout` / `drive not found` → peers can't talk (network state).
   - `unexpected drive ordering` / `Waiting for ... drives to come online` →
     slot↔node/volume mismatch.

3. **Are the nodes/placement healthy?** (rules out a down node / missing label)
   ```bash
   docker service ps copo_minio --no-trunc
   docker node ls
   docker node ls -q | xargs docker node inspect \
     -f '{{ .Description.Hostname }} | {{ .Spec.Labels }} | {{ .Spec.Availability }}'
   ```

4. **Is it the overlay or the underlay/firewall?** Decisive test — a *fresh*
   overlay between the same two nodes. If this pings fine, the wire/firewall is
   NOT the problem (don't touch firewall rules):
   ```bash
   docker network create -d overlay --attachable nettest
   docker service create --name nta --network nettest --endpoint-mode dnsrr \
     --constraint 'node.hostname==ei-copo-demo-frontend' nicolaka/netshoot sleep 3600
   docker service create --name ntb --network nettest --endpoint-mode dnsrr \
     --constraint 'node.hostname==ei-copo-demo-service'  nicolaka/netshoot sleep 3600
   sleep 8
   # ON ei-copo-demo-frontend (docker exec is node-local):
   docker exec $(docker ps -qf name=nta) ping -c3 ntb
   # cleanup (on a manager):
   docker service rm nta ntb && docker network rm nettest
   ```

---

## Things that DON'T fix it (tried, failed)

- `docker service update --force copo_minio` — doesn't rebuild network state.
- `systemctl restart docker` on the workers — overlay state is in the swarm
  network DB, not the local daemon; it re-syncs the stale state.
- Opening firewall ports (4789/udp, 7946) — the underlay was never blocked here;
  the fresh-overlay ping test confirms it.

---

## Notes

- **App config:** the app reaches MinIO via `ECS_ENDPOINT` (internal) and
  `ECS_ENDPOINT_EXTERNAL` (used for presigned URLs — must be browser-reachable;
  signed with SigV4 so the host can't be swapped after signing). See
  `common/s3/s3Connection.py`. Endpoints per environment are in the
  `copo.compose*.yaml` files.
- **Deploy drift:** the live demo has run image tags ahead of what the repo
  compose pins (e.g. `copo-new-web:v3.2.1.3` live vs `v3.2.0.1` in repo). When
  redeploying, use the tag you actually want live so you don't downgrade the app.
- **Production is moving to single-node** (decided 2026-09-01, after the failure
  recurred on prod through 2026-08-27). The compose change is made in
  `deployment/copo.compose.production.yaml`, but **do not deploy it on its own** —
  single-node cannot read the distributed pool's drives, so deploying without
  first migrating the objects destroys all production data. The full procedure,
  including the non-destructive parallel-mirror method and the IAM step that
  `mc mirror` does not cover, is in
  [`MINIO_SINGLE_NODE_MIGRATION.md`](MINIO_SINGLE_NODE_MIGRATION.md).
- The TL;DR wipe-the-volumes fix above is safe for **demo/dev only**, where losing
  objects is acceptable. Never apply it to production.
