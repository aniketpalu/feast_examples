"""
Negative E2E test: file-based store rejection by SparkApplicationComputeEngine.

Validates that the engine raises ValueError for every file-based store type,
both online (sqlite, faiss) and offline (dask, file, duckdb).

Run inside a pod that has the feast-spark-driver image (needs PySpark + our code).
"""

import sys
import traceback

from feast import RepoConfig


FILE_BASED_ONLINE = ["sqlite", "faiss"]
FILE_BASED_OFFLINE = ["dask", "file", "duckdb"]

BASE_CONFIG = {
    "project": "test_negative",
    "provider": "local",
    "registry": {"registry_type": "file", "path": "/tmp/registry.db"},
    "entity_key_serialization_version": 3,
    "auth": {"type": "no_auth"},
    "batch_engine": {
        "type": "spark_application",
        "image": "quay.io/aniket-redhat/feast-spark-driver:byos-spark-dev-1.0",
        "registry_address": "feast-server:6566",
    },
}


def test_rejects_file_based_online(store_type):
    config = {
        **BASE_CONFIG,
        "online_store": {"type": store_type},
        "offline_store": {"type": "spark"},
    }
    try:
        repo = RepoConfig(**config)
    except Exception as e:
        if "should end with" in str(e) or "not found" in str(e).lower():
            return True, f"RepoConfig rejects unknown store type '{store_type}': {e}"
        return False, f"RepoConfig error: {type(e).__name__}: {e}"

    from feast.infra.compute_engines.spark_application.compute import (
        SparkApplicationComputeEngine,
    )
    try:
        SparkApplicationComputeEngine(
            repo_config=repo, offline_store=None, online_store=None
        )
        return False, f"Expected ValueError for online_store={store_type}, but no error raised"
    except ValueError as e:
        if store_type.lower() in str(e).lower():
            return True, f"Correctly rejected: {e}"
        return False, f"ValueError raised but doesn't mention '{store_type}': {e}"
    except Exception as e:
        if "load_incluster_config" in str(e) or "kube" in str(e).lower():
            return False, f"Got past validation (K8s config error means store check passed): {e}"
        return False, f"Unexpected error: {type(e).__name__}: {e}"


def test_rejects_file_based_offline(store_type):
    config = {
        **BASE_CONFIG,
        "online_store": {"type": "redis", "connection_string": "redis:6379"},
        "offline_store": {"type": store_type},
    }
    repo = RepoConfig(**config)
    from feast.infra.compute_engines.spark_application.compute import (
        SparkApplicationComputeEngine,
    )
    try:
        SparkApplicationComputeEngine(
            repo_config=repo, offline_store=None, online_store=None
        )
        return False, f"Expected ValueError for offline_store={store_type}, but no error raised"
    except ValueError as e:
        if store_type.lower() in str(e).lower():
            return True, f"Correctly rejected: {e}"
        return False, f"ValueError raised but doesn't mention '{store_type}': {e}"
    except Exception as e:
        if "load_incluster_config" in str(e) or "kube" in str(e).lower():
            return False, f"Got past validation (K8s config error means store check passed): {e}"
        return False, f"Unexpected error: {type(e).__name__}: {e}"


def test_accepts_valid_stores():
    """Verify that valid (network-accessible) stores are NOT rejected."""
    config = {
        **BASE_CONFIG,
        "online_store": {"type": "redis", "connection_string": "redis:6379"},
        "offline_store": {"type": "spark"},
    }
    repo = RepoConfig(**config)
    from feast.infra.compute_engines.spark_application.compute import (
        SparkApplicationComputeEngine,
    )
    try:
        SparkApplicationComputeEngine(
            repo_config=repo, offline_store=None, online_store=None
        )
        return True, "Engine created (or failed on K8s config, not store validation)"
    except ValueError as e:
        if "file-based" in str(e).lower() or "sqlite" in str(e).lower():
            return False, f"Valid stores incorrectly rejected: {e}"
        return True, f"ValueError not related to store validation: {e}"
    except Exception as e:
        if "load_incluster_config" in str(e) or "kube" in str(e).lower():
            return True, "Passed store validation (K8s config error expected outside cluster)"
        return False, f"Unexpected error: {type(e).__name__}: {e}"


def main():
    print("=" * 70)
    print("NEGATIVE TEST: File-based store rejection")
    print("=" * 70)

    results = []

    print("\n--- Online store rejection ---")
    for store_type in FILE_BASED_ONLINE:
        passed, msg = test_rejects_file_based_online(store_type)
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] online_store={store_type}: {msg}")
        results.append((f"online:{store_type}", passed))

    print("\n--- Offline store rejection ---")
    for store_type in FILE_BASED_OFFLINE:
        passed, msg = test_rejects_file_based_offline(store_type)
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] offline_store={store_type}: {msg}")
        results.append((f"offline:{store_type}", passed))

    print("\n--- Valid stores accepted ---")
    passed, msg = test_accepts_valid_stores()
    status = "PASS" if passed else "FAIL"
    print(f"  [{status}] redis+spark: {msg}")
    results.append(("valid:redis+spark", passed))

    print("\n" + "=" * 70)
    total = len(results)
    passed_count = sum(1 for _, p in results if p)
    failed_count = total - passed_count
    print(f"RESULTS: {passed_count}/{total} passed, {failed_count} failed")

    if failed_count > 0:
        print("\nFAILED TESTS:")
        for name, p in results:
            if not p:
                print(f"  - {name}")

    print("=" * 70)
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
