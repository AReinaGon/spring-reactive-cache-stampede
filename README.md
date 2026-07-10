# Spring Reactive Cache Stampede PoC

A hands-on proof of concept demonstrating the **cache stampede** problem in a high-concurrency
ticketing platform and its mitigation with **reactive programming**.

## The problem

A *cache stampede* (a.k.a. dog-piling) is not caused by the absence of a cache — it happens **with**
a cache in place. A naive cache stores the *resolved value*, so an entry only exists once the
database has answered. When a popular key is cold or has just expired and a flood of concurrent
requests miss it at the same instant, every one of them queries the database simultaneously to
recompute the same value, because none of them has populated the entry yet. During a Black Friday
ticket drop this turns a single expired key into thousands of redundant database hits in
milliseconds, saturating the data source precisely when load is highest.

The fix demonstrated here is **"cache the promise, not the value"**: store the in-flight reactive
`Mono<T>` in the cache instead of its resolved result. The promise is published the instant the
first caller arrives, so every concurrent caller shares that same in-flight `Mono` and the database
is queried exactly once.

## Flow comparison

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

## Stack

| Technology         | Version          | Purpose                          |
|--------------------|------------------|----------------------------------|
| Java               | 25 (Corretto)    | Language baseline                |
| Spring Boot        | 4.0.6            | Application framework            |
| Spring WebFlux     | bundled          | Non-blocking reactive HTTP       |
| Project Reactor    | bundled          | Reactive operators (`Mono`)      |
| Caffeine           | 3.x              | In-memory local cache            |
| Spring Data R2DBC  | bundled          | Reactive PostgreSQL access       |
| PostgreSQL         | 17-alpine        | Database (Docker / Testcontainers) |
| Testcontainers     | 1.21.x           | Real PostgreSQL for integration tests |
| Actuator           | bundled          | Metrics & health endpoints       |

## Project layout

```
spring-reactive-cache-stampede-poc/
├── README.md
├── docker-compose.yml
└── reactive-local-caffeine/
    ├── pom.xml
    └── src/
        ├── main/java/com/areina/cachestampede/
        │   ├── CacheStampedeApplication.java
        │   ├── controller/TicketController.java
        │   ├── service/TicketValueCacheService.java    # naive value cache (stampedes)
        │   ├── service/TicketPromiseCacheService.java   # reactive promise cache (mitigates)
        │   ├── repository/TicketRepository.java         # reactive R2DBC repository (DatabaseClient)
        │   ├── model/TicketAvailability.java
        │   └── config/CacheConfig.java
        └── test/
            ├── java/com/areina/cachestampede/
            │   └── StampedeSimulationTest.java          # Testcontainers PostgreSQL integration test
            └── resources/db/schema.sql                  # table + seed (Testcontainers & docker-compose)
```

## Endpoints

| Method | Path                                          | Behaviour                                   |
|--------|-----------------------------------------------|---------------------------------------------|
| GET    | `/api/tickets/availability/{eventId}`         | Reactive promise cache (single DB hit)      |
| GET    | `/api/tickets/availability/{eventId}/naive`   | Naive value cache (stampedes on a cold key) |

## Running it

```bash
# 1. run the stampede simulation test
#    (needs a running Docker daemon — Testcontainers starts PostgreSQL automatically)
cd reactive-local-caffeine
mvn test

# 2. or run the app against a local PostgreSQL and probe the endpoints
docker-compose up -d   # starts PostgreSQL 17 and applies db/schema.sql
mvn spring-boot:run
curl http://localhost:8080/api/tickets/availability/black-friday-2026
curl http://localhost:8080/actuator/health
```

> The integration test owns its database: Testcontainers spins up a real PostgreSQL 17 container,
> applies `db/schema.sql`, and tears it down afterwards — so the only prerequisite is a running
> Docker daemon. `docker-compose up -d` is for running the app itself (e.g. to drive it with JMeter).

## What the test proves

`StampedeSimulationTest` fires **2,000 fully concurrent requests** at each service against the same
key (backed by a real PostgreSQL container) and counts how many times the database was actually queried:

```
[NAIVE VALUE CACHE]  DB queries: ~2000 | total time: Xms
[REACTIVE PROMISE]   DB queries: 1     | total time: Xms
```

The burst is driven at the service layer (via `Flux.range(0, 2000).flatMap(..., 2000)`) rather than
over HTTP, so the number reflects the caching strategy itself and not the connection limits of a
load-test client.

## Related article

[TODO] — Substack write-up link.

---

*Next: Phase 2 — scaling to distributed infrastructure with Redis and Redisson.*
