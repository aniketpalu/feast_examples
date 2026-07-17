# BatchFeatureView UDF (SparkApplication E2E)

`definitions.py` defines `udf_double_metrics` on `fv_1`.

## Constraints / driver fixes (byos-spark-e2e-v12)

1. **dill Python version** — apply UDF from driver Job (3.10), not feature-server (3.12).
2. **No nested `import pyspark` inside UDF** — can segfault after dill.
3. **Postgres empty feature_cols** — `mode=python` clears cols to mean SELECT *;
   driver patches `pull_latest` to `DISTINCT ON` / `a.*`.
4. **dill + Spark DataFrame ops segfault** — driver re-execs `udf_string` in
   `SparkTransformationNode` instead of calling the dill callable. Definitions
   must set `udf_string=dill.source.getsource(...)`.

```bash
oc apply -f e2e/apply_udf_bfv_driver_job.yaml
PYTHONPATH=/tmp/feast_merged_sdk python3 e2e/notebook_udf_bfv_test.py
```
