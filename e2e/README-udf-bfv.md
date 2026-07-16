# BatchFeatureView UDF (SparkApplication E2E)
#
# definitions.py defines `udf_double_metrics` (PySpark double_metrics UDF on fv_1).
#
# Python version constraint: feast-apply (feature-server image) and the Spark
# driver must share the same Python major.minor for dill-serialized UDFs.
# On current images (feature-server 3.12 vs driver 3.10), re-apply with:
#
#   oc delete job feast-udf-bfv-apply -n feast-spark --ignore-not-found
#   oc apply -f e2e/apply_udf_bfv_driver_job.yaml
#   oc logs -f job/feast-udf-bfv-apply -n feast-spark
#
# Then from the notebook (merged SDK with remote=True):
#
#   PYTHONPATH=/tmp/feast_merged_sdk python3 e2e/notebook_udf_bfv_test.py
#
# Expect: SparkApplication COMPLETED; online metric_a == 2*raw_a;
# metric_b == 2*raw_a + raw_b for entities 1..5.
