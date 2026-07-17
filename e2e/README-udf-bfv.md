# BatchFeatureView UDF (SparkApplication E2E)

`definitions.py` defines `udf_double_metrics` (PySpark `double_metrics` on `fv_1`).

## Driver image

Use `quay.io/aniket-redhat/feast-spark-driver:byos-spark-e2e-v13` (thin layer on v10) which includes:

1. Postgres `pull_latest` / `pull_all`: empty `feature_cols` → `SELECT *` / `DISTINCT ON`
   (`mode=python` clears cols to mean “all source columns”).
2. `SparkTransformationNode`: re-exec `udf_string` instead of calling dill callables
   (dill + Spark 4.0.1 DataFrame ops can segfault / exit 139).
3. Definitions must set `udf_string` **before** mainify (`__module__ = "__main__"`).

## Run order (important)

```bash
# 1) FeatureStore Ready with batch-engine CM pointing at v13
# 2) Re-apply UDF from driver image (overwrites feast-apply 3.12 dill with 3.10)
oc delete job feast-udf-bfv-apply -n feast-spark --ignore-not-found
oc apply -f e2e/apply_udf_bfv_driver_job.yaml
oc logs -f job/feast-udf-bfv-apply -n feast-spark

# 3) Materialize without restarting Feast (restart would re-run feast-apply 3.12)
PYTHONPATH=/tmp/feast_merged_sdk python3 e2e/notebook_udf_bfv_test.py
```

Expect: SparkApplication COMPLETED; online `metric_a == 2*raw_a`,
`metric_b == 2*raw_a + raw_b` for entities 1..5.

## Known constraints

| Issue | Mitigation |
|-------|------------|
| dill Python version (apply vs driver) | Driver Job apply after Ready |
| Nested `import pyspark` in UDF | Use `df["col"]` Column ops |
| Empty feature_cols + Postgres | Driver v13 postgres patch |
| dill DataFrame UDF segfault | `udf_string` re-exec in v13 |
| feast-apply after Ready overwrites UDF | Re-run driver Job before materialize |
