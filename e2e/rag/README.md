# Option A RAG E2E — SparkApplication materialize pre-embedded cities → Milvus

## Prereqs
- Namespace `feast-spark`, MinIO running, Feast Operator v18 + RELATED_IMAGE v18
- Images: `feature-server` / `feast-spark-driver` `:byos-spark-e2e-v18`

## Deploy

```bash
NS=feast-spark
oc project $NS

# Infra
oc apply -f e2e/rag/postgres.yaml
oc apply -f e2e/rag/milvus-secret.yaml
oc create sa milvus -n $NS 2>/dev/null || true
oc adm policy add-scc-to-user anyuid -z milvus -n $NS
oc apply -f e2e/rag/milvus-standalone.yaml

# Feast
oc apply -f e2e/rag/batch-engine-spark-rag.yaml
oc apply -f e2e/rag/featurestore-spark-rag.yaml
oc wait --for=jsonpath='{.status.phase}'=Ready featurestore/spark-rag -n $NS --timeout=300s

# Apply defs (driver Job — SparkSource needs JVM)
oc delete job spark-rag-apply -n $NS --ignore-not-found
oc apply -f e2e/rag/apply_driver_job.yaml
oc logs -f job/spark-rag-apply -n $NS

# Materialize + self-query
oc delete job spark-rag-e2e -n $NS --ignore-not-found
oc apply -f e2e/rag/test_rag_e2e_job.yaml
oc logs -f job/spark-rag-e2e -n $NS
```

## PASS
SparkApp COMPLETED + `retrieve_online_documents_v2` top-1 `item_id` matches parquet row 0.
