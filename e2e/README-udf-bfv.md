# BatchFeatureView UDF (SparkApplication E2E)
#
# definitions.py defines `udf_double_metrics` (PySpark double_metrics UDF on fv_1).
# The UDF is mainify'd (`__module__ = "__main__"`) for dill portability.
#
# Python version constraint: the process that runs `feast apply` for this UDF and
# the Spark driver image must share the same Python major.minor. feature-server
# feast-apply is often newer (e.g. 3.12) than the driver (e.g. 3.10). Re-apply with:
#
#   oc delete job feast-udf-bfv-apply -n feast-spark --ignore-not-found
#   oc apply -f e2e/apply_udf_bfv_driver_job.yaml
#   oc logs -f job/feast-udf-bfv-apply -n feast-spark
#
# The Job applies via SQL registry (postgres), not remote registry.
#
# Then from the notebook (merged SDK with remote=True):
#
#   PYTHONPATH=/tmp/feast_merged_sdk python3 e2e/notebook_udf_bfv_test.py
#
# Expect: SparkApplication COMPLETED; online metric_a == 2*raw_a;
# metric_b == 2*raw_a + raw_b for entities 1..5.
