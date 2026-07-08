# BYOS SparkApplication E2E Test Suite

Comprehensive end-to-end tests for the `spark_application` batch compute engine.

## Tests

### Negative: File-Based Store Rejection
`test_negative_file_stores.py` validates that `SparkApplicationComputeEngine` rejects
all file-based stores (pod has ephemeral filesystem):

| Store Type | Category | Reason |
|-----------|----------|--------|
| `sqlite` | online | Data written to pod-local SQLite lost on termination |
| `faiss` | online | Vector index files inaccessible across pods |
| `dask` | offline | Reads from local filesystem unavailable in pod |
| `file` | offline | Alias for dask, same limitation |
| `duckdb` | offline | Embedded file-based DB, pod-local |

### Positive: Real User Workflow
`test_client_e2e.py` runs from a **generic Python image** (no PySpark) and:
1. POSTs to `/materialize` on the Feast feature server
2. Server-side `store.materialize()` creates SparkApplication CR
3. Verifies FV states → AVAILABLE_ONLINE via remote registry
4. POSTs to `/get-online-features` to verify actual data

## Execution Order

```bash
# 1. Upload test data to MinIO
oc apply -f setup-data-pod.yaml
oc wait --for=condition=Ready pod/feast-e2e-setup-data

# 2. Deploy Feast via operator
oc apply -f batch-engine-configmap.yaml
oc apply -f featurestore-cr.yaml

# 3. Run negative tests (from feast-spark-driver image)
python test_negative_file_stores.py

# 4. Run positive tests (from generic Python image)
python test_client_e2e.py
```
