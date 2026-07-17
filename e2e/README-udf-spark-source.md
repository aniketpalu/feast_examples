# SparkSource BatchFeatureView UDF (v14)

Non-Postgres offline path: parquet on MinIO → SparkSource → SparkApplication → Redis.

## Images (this run)

| Role | Image | Digest |
|------|-------|--------|
| Feature server | `quay.io/aniket-redhat/feature-server:byos-spark-e2e-v14` | `sha256:d89edb6d…` |
| Spark driver | `quay.io/aniket-redhat/feast-spark-driver:byos-spark-e2e-v14` | `sha256:d2c0c7fa…` |

Tip: `merged-spark-e2e` @ `7e56462ab` (**stock** — no v13 UDF/postgres thin-layer patches).

## Run order

```bash
# 0) RELATED_IMAGE_FEATURE_SERVER → feature-server:byos-spark-e2e-v14
oc apply -f e2e/minio-spark-source.yaml
# create secret spark-offline-config (key: spark → spark_conf YAML) if missing
oc apply -f e2e/batch-engine-spark-source-udf.yaml
oc apply -f e2e/featurestore-spark-source-udf.yaml   # runFeastApplyOnInit: false

# Upload parquet (Job using setup_minio_small.py)
# Driver Python apply (dill + SparkSource.validate needs JVM)
oc delete job feast-udf-ss-apply -n feast-spark --ignore-not-found
oc apply -f e2e/apply_udf_spark_source_driver_job.yaml

# Notebook materialize
PYTHONPATH=/tmp/feast_merged_sdk python3 e2e/notebook_udf_spark_source_test.py
```

## E2E results (2026-07-17, `spark-src-udf`)

| Step | Result |
|------|--------|
| MinIO + parquet | PASS |
| FeatureStore Ready (v14 images) | PASS (with `runFeastApplyOnInit: false`) |
| Driver Job `feast apply` SparkSource+UDF | PASS (Python 3.10 + JVM) |
| Materialize `feature_view_1` (no UDF) | PASS SparkApp COMPLETED; Redis keys for `feast_spark_src_udf` |
| Online serve after COMPLETED | **FRICTION** — FV stuck not `AVAILABLE_ONLINE` (seen `MATERIALIZING` / `GENERATED` with intervals set) |
| Materialize `udf_double_metrics` | **FAIL** driver **ExitCode 139** |

## Frictions found (SparkSource path)

1. **feast-apply + SparkSource needs JVM**  
   `SparkSource.validate()` starts SparkSession. feature-server v14 has PySpark but no `JAVA_HOME` → CrashLoop.  
   Workaround: `runFeastApplyOnInit: false` + apply from driver image Job.

2. **Stock v14 has no UDF segfault mitigation**  
   `SparkTransformationNode` still calls dill `self.udf(*input_dfs)` — no `_resolve_udf` / `udf_string` re-exec (those were only in driver **v13** thin layer). BFV UDF → exit **139**.  
   Same class of bug as Postgres UDF path; **not Postgres-specific**.

3. **Online state lag / stuck after successful SparkApp** (known R1)  
   Non-UDF materialize COMPLETED and wrote Redis, but online `/get-online-features` 500 while state ≠ `AVAILABLE_ONLINE`.

## Contrast vs Postgres UDF E2E

| | Postgres offline + v13 | SparkSource + v14 |
|--|------------------------|-------------------|
| Empty `feature_cols` | Needed postgres patch | N/A (SparkSource) |
| UDF dill segfault | Fixed via `udf_string` in v13 | Reproduced on stock v14 |
| feast-apply JVM | Not required for PG source | Required for SparkSource.validate |
