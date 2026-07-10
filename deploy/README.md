# Deployments for the pod cluster (`reactive-distributed-redis`)

These manifests run the distributed module as **many real pods** sharing one Redis + one PostgreSQL,
so you can reproduce the cluster-wide stampede over HTTP (see [`../jmeter`](../jmeter)) instead of
simulating it in a single JVM like the integration test does.

Both deployments build the same image from [`../reactive-distributed-redis/Dockerfile`](../reactive-distributed-redis/Dockerfile),
which runs a **prebuilt jar** — build it once with your local Corretto 25:

```bash
cd ../reactive-distributed-redis
mvn -DskipTests package        # -> target/reactive-distributed-redis-1.0.0-SNAPSHOT.jar
```

## Option A — Docker Compose (simplest)

`docker/docker-compose.yml` runs Postgres + Redis + N app replicas behind an nginx round-robin load
balancer on `:8080`. The compose build does the image step for you:

```bash
cd docker
docker compose up --build --scale app=10 -d     # 10 pods
docker compose ps                                # see the replicas
curl http://localhost:8080/api/tickets/availability/black-friday-2026   # note "handledByPod"
```

- The `app` service has **no fixed host port and no container_name**, which is what lets it scale.
- nginx re-resolves the `app` service name every few seconds, so the burst spreads across all pods.
- `CACHE_VALUE_TTL` defaults to `2s` (short, so the stampede window keeps reopening). Override it:
  `CACHE_VALUE_TTL=10m docker compose up --build --scale app=10 -d`.

Tear down with `docker compose down -v`.

## Option B — Kubernetes

`k8s/` contains a Namespace, Postgres (+ schema ConfigMap), Redis, and the app Deployment/Service (configured as type `LoadBalancer`) plus a ConfigMap holding all connection + cache tuning.

```bash
# 1. build + load the image into your local cluster
cd ../reactive-distributed-redis && mvn -DskipTests package
docker build -t reactive-distributed-redis:local .
kind load docker-image reactive-distributed-redis:local        # or: minikube image load reactive-distributed-redis:local

# 2. deploy
kubectl apply -f ../deploy/k8s
kubectl scale deployment/app -n cache-stampede --replicas=20    # launch a ton of pods
kubectl get pods -n cache-stampede
```

### 3. Reach it

* **Docker Desktop**: Since the `app` service is of type `LoadBalancer`, Docker Desktop automatically maps this service to **`http://localhost:8080`** on your host Windows machine. No extra port-forwarding commands are needed!
* **Other clusters (Kind, minikube)**: If the External-IP is pending or not mapped automatically, you can run a port-forward fallback:
  ```bash
  kubectl port-forward -n cache-stampede svc/app 8080:8080
  ```

- Each pod injects its real name as `POD_NAME` (Downward API), so `handledByPod` in the JSON is the
  actual pod that served the request.
- Re-test different cache settings by editing `app-config` and rolling the deployment — no rebuild:
  ```bash
  kubectl edit configmap/app-config -n cache-stampede
  kubectl rollout restart deployment/app -n cache-stampede
  ```

## Measuring

Both options expose the same instrumentation the integration test uses, lifted to the cluster:

```bash
curl -X POST http://localhost:8080/api/tickets/cache/black-friday-2026/reset   # cold key + reset counter
# ... fire load (JMeter) ...
curl http://localhost:8080/api/tickets/stats                                   # {"dbHits": N}
```

Without the lock `dbHits` climbs toward the pod count; with it, it stays at one per TTL window.
