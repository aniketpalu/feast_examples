# SparkApplicationComputeEngine E2E Test

End-to-end validation of the `SparkApplicationComputeEngine` with per-FV result reporting
on OpenShift using the ODH/RHOAI Spark Operator.

## What this tests

1. **Batched materialization** — One `feast materialize` call → one SparkApplication pod
   processes all 10 feature views concurrently (5 executors, concurrency=10).
2. **Per-FV result reporting** — Pod calls `apply_materialization` via gRPC to the Feast
   registry after each successful FV, setting state to `AVAILABLE_ONLINE`.
3. **Large-scale data** — 10 FVs × 100,000 rows each = 1,000,000 total rows.
4. **ODH Spark Operator** — Uses RHOAI-provided Spark Operator (not upstream Helm install).

## Prerequisites

- OpenShift cluster with RHOAI/ODH installed (Spark Operator enabled)
- Feast Operator installed with a FeatureStore CR deployed
- MinIO (S3-compatible) for offline storage
- Redis for online storage
- `oc` logged in to the cluster

## Files

| File | Purpose |
|------|---------|
| `setup_data.py` | Generates 10 parquet datasets (100K rows each) in MinIO and registers FVs |
| `feature_store.yaml` | Feast config for the test project |
| `definitions.py` | Feature view and entity definitions |
| `test_materialize.py` | Runs materialization and verifies results |
| `setup-pod.yaml` | K8s Pod manifest to run setup_data.py in-cluster |

## How to run

```bash
# 1. Port-forward services
oc port-forward svc/feast-byos-spark-e2e-registry 6570:80 -n aniket-cursor-test &
oc port-forward svc/redis 6379:6379 -n aniket-cursor-test &
oc port-forward svc/minio 9000:9000 -n aniket-cursor-test &

# 2. Generate test data (run in-cluster via setup pod)
oc apply -f setup-pod.yaml -n aniket-cursor-test
oc logs -f feast-sparkapp-e2e-setup -n aniket-cursor-test

# 3. Run materialization test (from local machine)
python test_materialize.py
```

## Cluster details

- **Namespace**: `aniket-cursor-test`
- **Spark Operator**: ODH/RHOAI provided (`redhat-ods-applications` namespace)
- **Feast Image**: `quay.io/aniket-redhat/feature-server:byos-spark-dev-0.5`
- **Spark Driver Image**: `quay.io/aniket-redhat/feast-spark-driver:byos-spark-dev-0.9`
