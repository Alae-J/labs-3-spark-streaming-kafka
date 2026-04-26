"""
Provision Superset with the Spark SQL connection + datasets.

Idempotent: safe to re-run. Creates (or updates) one database connection
named 'Spark SQL' and three datasets (bronze_ticks, silver_ticks,
gold_moving_avg) pointing at the Hive Thrift endpoint exposed by
lab3-spark-thrift on port 10000.

Usage (from the host or from inside any lab3 container that can reach
http://superset:8088):

  docker exec lab3-superset python /opt/scripts/provision_superset.py
"""
import os
import sys
import time
import requests

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://superset:8088")
USER = os.environ.get("SUPERSET_USER", "admin")
PASS = os.environ.get("SUPERSET_PASS", "admin")

# SQLAlchemy URI for Spark Thrift (HiveServer2 protocol, SASL/NONE auth)
SQLALCHEMY_URI = "hive://hive@spark-thrift:10000/default?auth=NONE"
DB_NAME = "Spark SQL"

DATASETS = ["bronze_ticks", "silver_ticks", "gold_moving_avg"]


def wait_for_superset():
    """Block until Superset answers /health with HTTP 200."""
    for _ in range(60):
        try:
            r = requests.get(f"{SUPERSET_URL}/health", timeout=3)
            if r.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    sys.exit("Superset did not become healthy in 120 s")


def login() -> str:
    r = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": USER, "password": PASS,
              "provider": "db", "refresh": True},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["access_token"]


def csrf(token: str):
    """Get a CSRF token + a session that holds the cookie."""
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}"})
    r = s.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", timeout=10)
    r.raise_for_status()
    return r.json()["result"], s


def find_database(s: requests.Session, name: str) -> "int | None":
    r = s.get(f"{SUPERSET_URL}/api/v1/database/", timeout=10)
    r.raise_for_status()
    for entry in r.json().get("result", []):
        if entry["database_name"] == name:
            return entry["id"]
    return None


def create_or_update_database(s: requests.Session, csrf_token: str) -> int:
    payload = {
        "database_name": DB_NAME,
        "sqlalchemy_uri": SQLALCHEMY_URI,
        "expose_in_sqllab": True,
        "allow_run_async": False,
    }
    headers = {"X-CSRFToken": csrf_token, "Referer": SUPERSET_URL}
    existing = find_database(s, DB_NAME)
    if existing is not None:
        r = s.put(f"{SUPERSET_URL}/api/v1/database/{existing}",
                  json=payload, headers=headers, timeout=15)
        r.raise_for_status()
        print(f"  - updated existing database id={existing}")
        return existing
    r = s.post(f"{SUPERSET_URL}/api/v1/database/",
               json=payload, headers=headers, timeout=15)
    r.raise_for_status()
    db_id = r.json()["id"]
    print(f"  - created database id={db_id}")
    return db_id


def find_dataset(s: requests.Session, table_name: str) -> "int | None":
    r = s.get(f"{SUPERSET_URL}/api/v1/dataset/",
              params={"q": f"(filters:!((col:table_name,opr:eq,value:'{table_name}')))"},
              timeout=10)
    r.raise_for_status()
    for entry in r.json().get("result", []):
        if entry["table_name"] == table_name:
            return entry["id"]
    return None


def create_dataset(s: requests.Session, csrf_token: str, db_id: int, table: str) -> int:
    headers = {"X-CSRFToken": csrf_token, "Referer": SUPERSET_URL}
    existing = find_dataset(s, table)
    if existing is not None:
        print(f"  - dataset '{table}' already exists (id={existing}), skipping")
        return existing
    r = s.post(
        f"{SUPERSET_URL}/api/v1/dataset/",
        json={"database": db_id, "schema": "default", "table_name": table},
        headers=headers, timeout=20,
    )
    r.raise_for_status()
    ds_id = r.json()["id"]
    print(f"  - created dataset '{table}' id={ds_id}")
    return ds_id


def main():
    print(f"Waiting for Superset at {SUPERSET_URL} ...")
    wait_for_superset()

    print(f"Logging in as {USER} ...")
    token = login()
    csrf_token, s = csrf(token)

    print("Provisioning database connection ...")
    db_id = create_or_update_database(s, csrf_token)

    print("Provisioning datasets ...")
    for tbl in DATASETS:
        create_dataset(s, csrf_token, db_id, tbl)

    print("\nProvisioning complete.")
    print(f"  - Login at {SUPERSET_URL.replace('superset', 'localhost')} (admin/admin)")
    print("  - Database: 'Spark SQL'")
    print(f"  - Datasets: {', '.join(DATASETS)}")


if __name__ == "__main__":
    main()
