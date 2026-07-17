# BatchFeatureView UDF (SparkApplication E2E)

`definitions.py` defines `udf_double_metrics` (PySpark `double_metrics` on `fv_1`).
The UDF is mainify'd and avoids nested `from pyspark.sql import functions`
(dill + nested pyspark import segfaulted on Spark 4.0.1 / exit 139).

## Constraints

1. **Python version:** `feast apply` for this UDF and the Spark driver must share
   the same Python major.minor (dill bytecode). feature-server feast-apply is
   often 3.12 while the driver is 3.10 — re-apply with the driver Job below.
2. **Postgres empty feature_cols:** `mode=python` clears `feature_cols` to mean
   SELECT *. Postgres must treat that as `a.*` (fixed in driver
   `byos-spark-e2e-v11`).

## Run

```bash
oc delete job feast-udf-bfv-apply -n feast-spark --ignore-not-found
oc apply -f e2e/apply_udf_bfv_driver_job.yaml
oc logs -f job/feast-udf-bfv-apply -n feast-spark

# notebook (merged SDK with remote=True):
PYTHONPATH=/tmp/feast_merged_sdk python3 e2e/notebook_udf_bfv_test.py
```

Expect: SparkApplication COMPLETED; online `metric_a == 2*raw_a`,
`metric_b == 2*raw_a + raw_b` for entities 1..5.
