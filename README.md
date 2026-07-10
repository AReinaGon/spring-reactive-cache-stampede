# Spring Reactive Cache Stampede PoC

A hands-on proof of concept that reproduces the **cache stampede** problem in a high-concurrency
ticketing platform and demonstrates how to mitigate it with **reactive programming**, at two
different scales.

The repository contains **two independent, self-contained Spring Boot modules**. Each one can be
built and run on its own; they share nothing but the same idea and the same database schema.

| Module | Scale it reproduces | Mitigation it demonstrates |
|--------|---------------------|----------------------------|
| [`reactive-local-caffeine`](#reactive-local-caffeine) | Stampede **inside a single JVM** under massive concurrency | Cache the in-flight `Mono` (the promise) in a local Caffeine cache |
| [`reactive-distributed-redis`](#reactive-distributed-redis) | Stampede **across many replicas (pods)** of the service | A Redis-backed **distributed lock** (Redisson) elects one pod to repopulate the shared cache |

---

## What a cache stampede is

A *cache stampede* (a.k.a. dog-piling) is **not** caused by the absence of a cache — it happens
*with* a cache in place. A naive cache stores the *resolved value*, so an entry only exists once the
database has answered. When a popular key is cold or has just expired and a flood of concurrent
requests miss it at the same instant, every one of them queries the database simultaneously to
recompute the same value, because none of them has populated the entry yet. A single expired key
turns into thousands of redundant database hits in milliseconds, saturating the data source
precisely when load is highest.

Both modules contrast a **naive** implementation (which stampedes) against a **mitigated** one, and
an integration test counts how many times the database is actually queried so the effect is
measurable, not just described.

---

## `reactive-local-caffeine`

### What it simulates

A **single JVM** receiving a burst of fully concurrent requests for the same cold key. The fix is
**"cache the promise, not the value"**: store the in-flight reactive `Mono<T>` in the cache instead
of its resolved result. The promise is published the instant the first caller arrives, so every
concurrent caller shares that same in-flight `Mono` and the database is queried exactly once.

```
NAIVE VALUE CACHE (TicketValueCacheService)   REACTIVE PROMISE CACHE (TicketPromiseCacheService)
Cache<String, TicketAvailability>             Cache<String, Mono<TicketAvailability>>

 req 1 ─┐ all miss the cold key                 req 1 ─┐
 req 2 ─┤ (value not stored until               req 2 ─┤   get(key, k -> repo.find(k).cache())
  ...   ├─► the DB answers) ─► DB query           ...   ├─►          │
 req N ─┘        (N queries)                     req N ─┘            ▼
                                                            single shared Mono published
        DB ◄── query ◄── query ◄── query                   on the first call
              (DB hammered: N hits)                                  │
                                                                     ▼
                                                           DB ◄── 1 query (1 hit)
                                                      result replayed to all N callers
```

### Endpoints

| Method | Path                                          | Behaviour                                   |
|--------|-----------------------------------------------|---------------------------------------------|
| GET    | `/api/tickets/availability/{eventId}`         | Reactive promise cache (single DB hit)      |
| GET    | `/api/tickets/availability/{eventId}/naive`   | Naive value cache (stampedes on a cold key) |

### Run it

```bash
cd reactive-local-caffeine

# Option A — run the stampede simulation test.
# Testcontainers starts a real PostgreSQL automatically; the only prerequisite is a running Docker daemon.
mvn test

# Option B — run the app against a local PostgreSQL and probe the endpoints.
docker-compose up -d   # from the repo root: starts PostgreSQL 17 and applies db/schema.sql
mvn spring-boot:run
curl http://localhost:8080/api/tickets/availability/black-friday-2026
curl http://localhost:8080/actuator/health
```

`StampedeSimulationTest` fires **2,000 fully concurrent requests** at each service against the same
key (backed by a real PostgreSQL container) and counts the database queries. The burst is driven at
the service layer (via `Flux.range(0, 2000).flatMap(..., 2000)`) rather than over HTTP, so the
number reflects the caching strategy itself and not the connection limits of a load-test client:

```
[NAIVE VALUE CACHE]  DB queries: ~2000 | total time: Xms
[REACTIVE PROMISE]   DB queries: 1     | total time: Xms
```

---

## `reactive-distributed-redis`

### What it simulates

The same service running as **many replicas (pods)**, where the stampede reappears one level up. A
`Mono` lives on a single pod's heap and **cannot be shared across JVMs**, so when a popular key
expires every pod regenerates it independently — up to one redundant database query *per pod*, per
expiry. The fix is a **distributed lock**: a single pod is elected to query the database and
repopulate the shared Redis cache, while the rest wait and read the populated value.

```
NO LOCK (TicketNoLockCacheService)            DISTRIBUTED LOCK (TicketLockCacheService)

 pod 1 ─┐ all miss the cold Redis key          pod 1 ─┐ all miss → compete for one Redis lock
 pod 2 ─┤ (value not stored until              pod 2 ─┤   winner: query DB → SET key → unlock
  ...   ├─► the DB answers) ─► DB query          ...   ├─► losers: acquire → re-check key → HIT
 pod N ─┘        (N queries)                    pod N ─┘            │
                                                                    ▼
        DB ◄── query ◄── query ◄── query                  DB ◄── 1 query for the whole cluster
              (DB hammered: N hits)
```

Two implementation details make the mitigated count land on exactly one query:

- **Double-checked read.** After acquiring the lock, a pod reads Redis *again*; a loser that waited
  now finds the value the winner wrote and returns it without touching the database.
- **Explicit `threadId`.** Reactor may run the lock-acquire and the unlock on different scheduler
  threads, and a Redisson lock is owned by `(clientId, threadId)` — unlocking from a different thread
  throws. Each request mints a stable id and passes it to both `tryLock` and `unlock`.

#### Why Redisson (and not Spring Data Redis)?

The deciding factor is the **distributed lock**, not the value cache. Redisson is the only option
offering a *reactive* distributed lock with watchdog lease renewal and owner-only safe release, so
this module uses it for **both** the lock (`RLockReactive`) and the value cache (`RBucketReactive`)
— one cohesive client, no Spring Data Redis on the classpath. The two services then differ in
exactly one thing: the lock.

> The value is stored as plain JSON via Redisson's `StringCodec` (Spring Boot 4 ships Jackson 3,
> while Redisson's bundled JSON codec targets Jackson 2 and its Kryo5 codec cannot instantiate Java
> records). The JSON stays readable under `redis-cli` for inspection.

#### A third variant: local promise cache + distributed lock

The distributed lock alone already guarantees one DB query for the cluster, but it leaves waste
*inside* each pod: every concurrent same-pod request mints its own lock owner, so they all contend
for the same Redis lock and serialize through it — even though they want the very same value. That is
the single-JVM stampede again, one level down.

`TicketLayeredCacheService` puts a **per-pod local promise cache in front of the lock**, reusing the
"cache the promise, not the value" idea locally so that only **one representative request per pod**
ever reaches the distributed lock; the rest share the same in-flight `Mono`.

This needs two pieces:

- Reactor's `.cache()` shares *subscribers of one `Mono`* — but each call builds a new `Mono`, so on
  its own it does **not** deduplicate independent requests.
- A keyed concurrent store that hands every concurrent same-pod caller the *same* in-flight `Mono`
  (its mapping function must run at most once per key). A plain `ConcurrentHashMap.computeIfAbsent`
  would do this with **no extra dependency**; this module uses **Caffeine** for the same job because
  it adds a bounded safety-net eviction (`expireAfterWrite` / `maximumSize`) for the edge cases a bare
  map cannot bound, and keeps the "cache the promise" layer symmetric with `reactive-local-caffeine`.

The local entry is **ephemeral**: it is evicted the instant the shared `Mono` terminates
(`doFinally(invalidate)`), so Redis stays the single durable, shared cache and there is no local
staleness — this layer is pure in-flight request coalescing.

### Endpoints

| Method | Path                                          | Behaviour                                                  |
|--------|-----------------------------------------------|------------------------------------------------------------|
| GET    | `/api/tickets/availability/{eventId}`         | Distributed lock (one DB hit for the cluster)              |
| GET    | `/api/tickets/availability/{eventId}/layered` | Local promise cache **+** distributed lock (coalesces same-pod concurrency too) |
| GET    | `/api/tickets/availability/{eventId}/naive`   | Naive Redis value cache (stampedes across pods)            |

### Run it

```bash
cd reactive-distributed-redis

# Option A — run the integration test.
# Testcontainers starts BOTH PostgreSQL and Redis automatically; only a running Docker daemon is needed.
mvn test

# Option B — run the app (Redis must be up first: Redisson connects eagerly on startup).
docker-compose up -d        # from the repo root: starts PostgreSQL 17 + Redis 7
mvn spring-boot:run
curl http://localhost:8080/api/tickets/availability/black-friday-2026
```

> Both modules default to port 8080, so run them one at a time. To run this one alongside the other,
> start it on a different port: `mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8081`.

`DistributedStampedeSimulationTest` simulates a **20-pod cluster inside one JVM**: 20 service
instances with distinct ids, all sharing the same Redis (one Redisson client) and the same database
counter (so the counter measures the load the database sees from the whole cluster). The inter-pod
tests fire one request per pod at the same cold key, all at once:

```
[NO DISTRIBUTED LOCK] DB queries: 20 | simulated pods: 20
[DISTRIBUTED LOCK]    DB queries: 1  | simulated pods: 20
```

A third test isolates the **intra-pod** dimension: a single pod fires 50 concurrent requests at the
cold key and counts the *lock attempts*. The lock alone still lands on one DB query, but every
request contends for the lock; the layered service collapses the whole burst into a single lock
attempt:

```
[LOCK ONLY]   requests: 50 | DB queries: 1 | lock attempts: ~41
[LOCAL+LOCK]  requests: 50 | DB queries: 1 | lock attempts: 1
```

The two layers are complementary in production: the local promise cache collapses the concurrency
*inside* each pod (one representative request per pod reaches the lock), and the distributed lock
ensures only one pod of the cluster repopulates the shared cache. The lock is required for
correctness; the local cache is an optimization that stops the lock from becoming an internal
bottleneck under heavy per-pod concurrency.

---

## Stack

| Technology         | Version          | Used by                          |
|--------------------|------------------|----------------------------------|
| Java               | 25 (Corretto)    | both                             |
| Spring Boot        | 4.0.6            | both                             |
| Spring WebFlux     | bundled          | both (non-blocking reactive HTTP)|
| Project Reactor    | bundled          | both (`Mono` operators)          |
| Caffeine           | 3.x              | both (`reactive-local-caffeine`: main local cache; `reactive-distributed-redis`: per-pod promise cache in front of the lock) |
| Redisson           | 4.4.0            | `reactive-distributed-redis` (reactive distributed lock + value cache) |
| Redis              | 7-alpine         | `reactive-distributed-redis` (distributed cache & lock arbiter) |
| Spring Data R2DBC  | bundled          | both (reactive PostgreSQL access)|
| PostgreSQL         | 17-alpine        | both (Docker / Testcontainers)   |
| Testcontainers     | 1.21.x           | both (real PostgreSQL; + real Redis for the distributed module) |
| Actuator           | bundled          | both (metrics & health endpoints)|

## Shared infrastructure

The integration tests own their own infrastructure: Testcontainers spins up real PostgreSQL (and
Redis, for the distributed module), applies `db/schema.sql`, and tears everything down afterwards —
so the only prerequisite for `mvn test` is a running Docker daemon.

`docker-compose up -d` (from the repo root) is only for running the apps by hand. It starts:

- **PostgreSQL 17** on port 5432, initialised with `db/schema.sql` (table `ticket_availability`
  seeded with `black-friday-2026`).
- **Redis 7** on port 6379 (needed by `reactive-distributed-redis`).

## Project layout

```
spring-reactive-cache-stampede-poc/
├── README.md
├── docker-compose.yml                                  # PostgreSQL + Redis for running the apps by hand
├── reactive-local-caffeine/                            # single-JVM stampede (Caffeine promise cache)
│   ├── pom.xml
│   └── src/
│       ├── main/java/com/areina/cachestampede/
│       │   ├── CacheStampedeApplication.java
│       │   ├── controller/TicketController.java
│       │   ├── service/TicketValueCacheService.java    # naive value cache (stampedes)
│       │   ├── service/TicketPromiseCacheService.java   # reactive promise cache (mitigates)
│       │   ├── repository/TicketRepository.java         # reactive R2DBC repository (DatabaseClient)
│       │   ├── model/TicketAvailability.java
│       │   └── config/CacheConfig.java
│       └── test/
│           ├── java/com/areina/cachestampede/
│           │   └── StampedeSimulationTest.java          # Testcontainers PostgreSQL integration test
│           └── resources/db/schema.sql                  # table + seed (Testcontainers & docker-compose)
└── reactive-distributed-redis/                         # cluster-wide stampede (Redis + Redisson lock)
    ├── pom.xml
    └── src/
        ├── main/java/com/areina/distributedlock/
        │   ├── DistributedLockApplication.java
        │   ├── controller/TicketController.java
        │   ├── service/TicketNoLockCacheService.java    # naive Redis value cache (stampedes across pods)
        │   ├── service/TicketLockCacheService.java       # distributed lock (collapses to 1 DB query)
        │   ├── service/TicketLayeredCacheService.java    # local promise cache + distributed lock (also coalesces intra-pod)
        │   ├── repository/TicketRepository.java
        │   ├── model/TicketAvailability.java             # adds handledByPod
        │   └── config/{RedissonConfig, TicketJsonCodec}.java
        └── test/
            ├── java/com/areina/distributedlock/
            │   └── DistributedStampedeSimulationTest.java # Testcontainers PostgreSQL + Redis
            └── resources/db/schema.sql
```
