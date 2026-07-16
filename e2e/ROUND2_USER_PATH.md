# Round 2 — correct user path (no workarounds)
#
# Prerequisites (platform/admin, already done on this cluster):
#   - Feast Operator with RELATED_IMAGE_FEATURE_SERVER=<feature-server image>
#   - ODH Spark Operator available
#   - Secrets: postgres-offline-config, postgres-registry-config, redis-config
#   - Postgres + Redis running
#
# User steps:
#   1. Load offline store tables FIRST (setup_postgres_small.py — default 10 FVs)
#      feast-apply introspects PostgreSQLSource tables; missing tables → CrashLoop
#   2. oc apply -f e2e/batch-engine-spark-pg-e2e.yaml
#   3. oc apply -f e2e/featurestore-spark-pg-e2e.yaml
#   4. Wait FeatureStore Ready
#   5. oc apply -f e2e/feast-rest-route.yaml   # optional Swagger routes
#   6. From notebook: materialize(..., remote=True)
#      Requires a Feast client build that supports remote=True (#6590)
#
# Intentionally omitted:
#   - service_account in batch ConfigMap (operator creates feast-<cr>-batch-driver)
#   - initImage / server.image (RELATED_IMAGE_FEATURE_SERVER)
#   - Manual Feast RBAC
