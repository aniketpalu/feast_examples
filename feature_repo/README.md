# Option A — RAG feature repo (SparkApplication, no embed UDF)

Branch: `feast-rag` on [aniketpalu/feast_examples](https://github.com/aniketpalu/feast_examples).

## What Spark computes

**Nothing generative.** Materialize reads pre-computed 384-d vectors from MinIO and writes them to the online vector store (Milvus). Embeddings were produced offline (Feast `examples/rag` city Wikipedia data).

```
city_embeddings.parquet (vector already present)
        │  SparkSource
        ▼
feast materialize  →  SparkApplication  →  Milvus
        │
        ▼
retrieve_online_documents_v2(query=row.vector)  # self-query PASS
```

## Files

| File | Role |
|------|------|
| `definitions.py` | Entity `item_id` + FeatureView `city_embeddings` + SparkSource |
| `data/city_embeddings.parquet` | 811 rows, 384-d vectors, Spark-safe µs UTC timestamps |
| `prepare_data.py` | Regenerate parquet / optional MinIO upload |

## Schema

- **Entity:** `item_id` (INT64)
- **Features:** `vector` (Array[Float32], COSINE index), `state`, `sentence_chunks`, `wiki_summary`
- **Source path:** `s3a://feast-rag/city_embeddings.parquet`

## Local data prep

```bash
cd feature_repo
# already checked in; to rebuild from upstream Feast examples/rag:
python prepare_data.py --src /path/to/city_wikipedia_summaries_with_embeddings.parquet

# upload to MinIO (port-forward or in-cluster endpoint)
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
python prepare_data.py --upload --endpoint http://127.0.0.1:9000
```

## Next steps (not in this commit)

1. Milvus deploy + FeatureStore CR (`batch_engine: spark_application`)
2. `feast apply` via driver Job (`runFeastApplyOnInit: false` — SparkSource needs JVM)
3. Remote materialize + self-query E2E

See nous: `projects/feast/technical/features/byos-spark/proposal-rag-sparkapp-oneshot.md`.
