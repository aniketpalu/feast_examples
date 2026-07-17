# SparkSource BatchFeatureView UDF (v14)

Non-Postgres offline path: parquet on MinIO → SparkSource → SparkApplication → Redis.

## Images

- Feature server: `quay.io/aniket-redhat/feature-server:byos-spark-e2e-v14`
- Spark driver: `quay.io/aniket-redhat/feast-spark-driver:byos-spark-e2e-v14`

## Run order

```bash
# 0) RELATED_IMAGE_FEATURE_SERVER → feature-server:byos-spark-e2e-v14
# 1) MinIO + spark-offline-config secret + batch-engine CM + FeatureStore CR
oc apply -f e2e/minio-spark-source.yaml
# create secret spark-offline-config (spark_conf YAML) if missing
oc apply -f e2e/batch-engine-spark-source-udf.yaml
oc apply -f e2e/featurestore-spark-source-udf.yaml

# 2) Upload parquet
oc run minio-setup --rm -it --restart=Never -n feast-spark \
  --image=quay.io/aniket-redhat/feature-server:byos-spark-e2e-v14 \
  -- python3 -c '...'  # or copy e2e/setup_minio_small.py

# 3) Driver Python apply (dill match)
oc delete job feast-udf-ss-apply -n feast-spark --ignore-not-found
oc apply -f e2e/apply_udf_spark_source_driver_job.yaml

# 4) Notebook materialize (no Feast restart)
PYTHONPATH=/tmp/feast_merged_sdk python3 e2e/notebook_udf_spark_source_test.py
```
