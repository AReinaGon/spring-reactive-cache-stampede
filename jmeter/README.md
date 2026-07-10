# JMeter load tests (Apache JMeter 5.6.3)

Two test plans that reproduce, over real HTTP, the same stampede the integration tests reproduce at
the service layer:

| Plan | Targets | What it shows |
|------|---------|---------------|
| `distributed-stampede.jmx` | `reactive-distributed-redis` (a cluster of pods behind a load balancer) | One cold key, many pods: the **cluster-wide DB-hit count** with vs. without the distributed lock |
| `local-stampede.jmx` | `reactive-local-caffeine` (single JVM) | Throughput/latency of the promise cache vs. the naive value cache under load |

> The plans are authored for **JMeter 5.6.3**. Install it (`brew install jmeter`, `choco install jmeter`,
> or download the binary from Apache) and make sure `jmeter` is on your `PATH`.

## Properties (override with `-J`)

Every plan reads its parameters via `${__P(name,default)}`, so nothing is hard-coded:

| Property | Default | Meaning |
|----------|---------|---------|
| `host` | `localhost` | target host (the load balancer / port-forward) |
| `port` | `8080` | target port |
| `eventId` | `black-friday-2026` | the key to hammer (seeded in `db/schema.sql`) |
| `endpoint` | `` (empty) | `` = distributed lock · `/naive` = no coordination · `/layered` = local promise cache + lock |
| `threads` | `200` | concurrent virtual users |
| `rampup` | `1` | ramp-up seconds (1 ≈ everyone at once) |
| `loops` | `1` (distributed) / `5` (local) | iterations per thread |

Example: 500 users, all at once, against the naive endpoint:

```bash
jmeter -n -t distributed-stampede.jmx -Jthreads=500 -Jrampup=1 -Jendpoint=/naive -l results-naive.jtl
```

## Running the distributed plan

### 1. Bring up a pod cluster

Pick one of the deployments in [`../deploy`](../deploy):

```bash
# Docker (simplest): 10 pods behind nginx on :8080
cd ../deploy/docker
docker compose up --build --scale app=10 -d
```

```bash
# Kubernetes: 10 pods, then forward the service to localhost:8080
kubectl apply -f ../deploy/k8s
kubectl scale deployment/app -n cache-stampede --replicas=10
kubectl port-forward -n cache-stampede svc/app 8080:8080
```

### 2. Fire the burst (headless)

The plan's **setUp** thread group calls `POST /api/tickets/cache/${eventId}/reset` first — that
deletes the cached value (forces a cold key) and zeroes the cluster-wide DB-hit counter — then the
main group fires the burst, and the **tearDown** group reads `GET /api/tickets/stats` back and logs
it as `[JMETER] cluster DB hits = {...}`.

```bash
# No coordination — every pod that misses the cold key hits the DB (stampede).
jmeter -n -t distributed-stampede.jmx -Jthreads=200 -Jendpoint=/naive -l naive.jtl

# Distributed lock — the whole cluster collapses onto a single DB query.
jmeter -n -t distributed-stampede.jmx -Jthreads=200 -Jendpoint= -l lock.jtl

# Local promise cache + lock — same single query, far less lock contention per pod.
jmeter -n -t distributed-stampede.jmx -Jthreads=200 -Jendpoint=/layered -l layered.jtl
```

### 3. Read the result

```bash
curl http://localhost:8080/api/tickets/stats
# {"dbHits": ...}   <- also printed by the tearDown group in the JMeter log
```

Expected, mirroring the integration test:

```
/naive    -> dbHits ≈ number of pods   (a redundant query per pod that missed the cold key)
(lock)    -> dbHits = 1                 (one pod repopulates the shared cache for everyone)
/layered  -> dbHits = 1                 (same, plus same-pod requests never re-contend the lock)
```

## TTL: clean shot vs. repeated stampede

The deployments set a short `CACHE_VALUE_TTL` (`2s`) on purpose, so you don't wait out the 10-minute
production default to watch a key go cold again. That gives you two ways to test:

- **One clean measurement** — set a long TTL so the key stays cached for the whole run:
  `docker compose up --build --scale app=10 -d` with `CACHE_VALUE_TTL=10m` (or edit the k8s ConfigMap).
  The counter then lands on exactly `pods` (naive) vs `1` (lock).
- **Sustained pressure** — keep the short TTL; the key keeps expiring mid-run and the counter keeps
  climbing, fast for naive and slowly (≈ one per TTL window) for the lock. This is the more realistic
  "popular key under constant load" picture.

You can also reset between runs manually instead of relying on setUp:

```bash
curl -X POST http://localhost:8080/api/tickets/cache/black-friday-2026/reset
# or flush everything in Redis:
docker exec -it $(docker ps -qf name=redis) redis-cli FLUSHALL
```

## Running the local plan

```bash
cd ../reactive-local-caffeine && mvn spring-boot:run     # single JVM on :8080
# in another shell:
cd jmeter
jmeter -n -t local-stampede.jmx -Jthreads=300 -Jendpoint=/naive -l local-naive.jtl
```

The single-JVM module exposes no `/stats` endpoint (its DB-hit proof is `StampedeSimulationTest`), so
here JMeter measures **throughput and latency**. The naive value cache only stampedes against a *cold*
cache, i.e. immediately after the app starts; once warm both endpoints serve from cache.

## Opening results in the GUI

`-l results.jtl` writes raw samples. Inspect them later with:

```bash
jmeter -g results.jtl -o report/      # generates an HTML dashboard in report/
```

Or open `*.jmx` in the JMeter GUI to edit threads/assertions interactively (run headless for real
load — the GUI is for authoring only).
