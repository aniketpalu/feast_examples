# Local-engine E2E (no SparkApplication / no batchEngine)
#
# 1. Load offline tables: python3 e2e/setup_postgres_small.py  (NUM_FVS=10 default)
# 2. oc apply -f e2e/featurestore-pg-local-e2e.yaml
# 3. Wait FeatureStore Ready
# 4. oc apply -f e2e/feast-rest-route.yaml
# 5. Notebook: materialize(..., remote=True) — expect NO SparkApplication CRs
# 6. get_online_features including metric_sum_odfv:metric_sum
