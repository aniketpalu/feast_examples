"""Option A RAG E2E: remote materialize city_embeddings → self-query vector search."""

from __future__ import annotations

import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from feast import FeatureStore

# Client talks to feature server for remote materialize; registry SQL for state.
ONLINE_URL = os.environ.get(
    "FEAST_ONLINE_URL",
    "https://feast-spark-rag-online.feast-spark.svc.cluster.local:443",
)
CA = os.environ.get("FEAST_CA_CERT_FILE_PATH", "")
PG_USER = os.environ.get("PG_USER", "feast")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "feast")
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "feast")
MILVUS_HOST = os.environ.get("MILVUS_HOST", "http://milvus")
MILVUS_PORT = int(os.environ.get("MILVUS_PORT", "19530"))
PARQUET = os.environ.get(
    "PARQUET_PATH",
    "/data/city_embeddings.parquet",
)


def _client_yaml(path: Path) -> None:
    registry = (
        f"postgresql+psycopg://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    cert_line = f"  cert: {CA}\n" if CA else ""
    path.write_text(
        f"""project: feast_spark_rag
provider: local
offline_store:
  type: spark
  spark_conf:
    spark.master: local[*]
online_store:
  type: milvus
  host: {MILVUS_HOST}
  port: {MILVUS_PORT}
  vector_enabled: true
  embedding_dim: 384
  index_type: FLAT
  metric_type: COSINE
registry:
  registry_type: sql
  path: {registry}
feature_server:
  type: local
  url: "{ONLINE_URL}"
{cert_line}entity_key_serialization_version: 3
auth:
  type: no_auth
"""
    )


def main() -> None:
    parquet = Path(PARQUET)
    if not parquet.exists():
        raise SystemExit(f"missing parquet {parquet}")

    table = pq.read_table(parquet)
    item_id = table.column("item_id")[0].as_py()
    query_vec = table.column("vector")[0].as_py()
    print(f"self-query item_id={item_id} dim={len(query_vec)}", flush=True)

    with tempfile.TemporaryDirectory(prefix="rag-e2e-") as tmp:
        repo = Path(tmp)
        _client_yaml(repo / "feature_store.yaml")
        store = FeatureStore(repo_path=str(repo))

        start = datetime(1970, 1, 1, tzinfo=timezone.utc)
        end = datetime.now(timezone.utc)
        print("=== materialize city_embeddings (remote/async) ===", flush=True)
        try:
            store.materialize(
                start_date=start,
                end_date=end,
                feature_views=["city_embeddings"],
                run_async=True,
                disable_event_timestamp=True,
            )
        except TypeError:
            # older SDK
            store.materialize(
                start_date=start,
                end_date=end,
                feature_views=["city_embeddings"],
                disable_event_timestamp=True,
            )

        # Poll FV state
        deadline = time.time() + 600
        while time.time() < deadline:
            fv = store.get_feature_view("city_embeddings")
            state = getattr(fv, "state", None) or getattr(
                getattr(fv, "meta", None), "state", None
            )
            print(f"state={state}", flush=True)
            if str(state).endswith("AVAILABLE_ONLINE") or state == 4:
                break
            time.sleep(5)
        else:
            raise SystemExit("TIMEOUT waiting AVAILABLE_ONLINE")

        print("=== retrieve_online_documents_v2 (self-query) ===", flush=True)
        ok = False
        for attempt in range(1, 16):
            try:
                results = store.retrieve_online_documents_v2(
                    features=[
                        "city_embeddings:vector",
                        "city_embeddings:state",
                        "city_embeddings:sentence_chunks",
                    ],
                    query=query_vec,
                    top_k=3,
                ).to_dict()
                ids = results.get("item_id") or results.get("item_id_pk") or []
                print(f"attempt={attempt} ids={ids}", flush=True)
                if ids and int(ids[0]) == int(item_id):
                    ok = True
                    break
            except Exception as e:
                print(f"attempt={attempt} err={e}", flush=True)
            time.sleep(2)

        if not ok:
            raise SystemExit("FAIL: self-query top-1 mismatch")
        print("PASS: SparkApplication RAG Option A self-query", flush=True)


if __name__ == "__main__":
    main()
