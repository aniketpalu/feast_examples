"""Load a SMALL Postgres dataset for functionality E2E (not scale).

Default: 10 tables x 1,000 rows x 100 entities (matches feature_repo FV count).
Run inside the cluster (notebook or a one-shot pod) with network to postgres:5432.

Load these offline tables BEFORE applying the FeatureStore CR — feast-apply
introspects PostgreSQLSource tables and CrashLoops if they are missing.
"""

from __future__ import annotations

import os
import sys

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

PG_URL = os.environ.get(
    "PG_URL",
    "postgresql+psycopg://feast:feast@postgres:5432/feast",
)
NUM_FVS = int(os.environ.get("NUM_FVS", "10"))
ROWS_PER_FV = int(os.environ.get("ROWS_PER_FV", "1000"))
NUM_ENTITIES = int(os.environ.get("NUM_ENTITIES", "100"))
CATEGORIES = ["electronics", "clothing", "food", "automotive", "software"]


def main() -> None:
    assert ROWS_PER_FV % NUM_ENTITIES == 0, "ROWS_PER_FV must be divisible by NUM_ENTITIES"
    engine = create_engine(PG_URL)
    ts_per_entity = ROWS_PER_FV // NUM_ENTITIES
    base_ts = pd.Timestamp("2024-01-01", tz="UTC")

    print(f"Loading {NUM_FVS} tables x {ROWS_PER_FV} rows into {PG_URL.split('@')[-1]}")
    with engine.begin() as conn:
        for i in range(1, NUM_FVS + 1):
            np.random.seed(i * 42)
            entity_ids = np.repeat(np.arange(1, NUM_ENTITIES + 1), ts_per_entity)
            timestamps = np.tile(
                pd.date_range(base_ts, periods=ts_per_entity, freq="h"),
                NUM_ENTITIES,
            )
            df = pd.DataFrame(
                {
                    "entity_id": entity_ids,
                    "event_timestamp": timestamps,
                    "metric_a": np.random.random(ROWS_PER_FV),
                    "metric_b": np.random.random(ROWS_PER_FV) * 100,
                    "metric_c": np.random.random(ROWS_PER_FV) * 1000,
                    "category": np.random.choice(CATEGORIES, ROWS_PER_FV),
                    "score": np.random.random(ROWS_PER_FV),
                }
            )
            table = f"fv_{i}"
            conn.execute(text(f"DROP TABLE IF EXISTS {table}"))
            df.to_sql(table, conn, index=False, if_exists="replace")
            n = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"  {table}: {n} rows")
    print("Done")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FAILED: {e}", file=sys.stderr)
        raise
