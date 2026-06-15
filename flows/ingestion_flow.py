import os
os.environ["PREFECT_API_URL"] = ""

import subprocess
from prefect import flow, task
import pandas as pd
import duckdb
from sqlalchemy import create_engine
from minio import Minio
from io import BytesIO
from soda.scan import Scan

# -------------------------
# CONFIG
# -------------------------

BASE_DIR      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DUCKDB_PATH   = os.path.join(BASE_DIR, "warehouse.duckdb")
POSTGRES_CONN = "postgresql://dataops:dataops@127.0.0.1:5433/oltp"
SODA_CONFIG   = os.path.join(BASE_DIR, "soda", "configuration.yml")
DBT_PROFILES  = os.path.expanduser("~/.dbt")
VENV_BIN      = os.path.join(BASE_DIR, "dbt-env", "bin")

MINIO_CLIENT = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)
BUCKET = "data-store"

EXPECTED_SCHEMAS = {
    "orders":      {"order_id", "customer_id", "store_id", "order_date", "status"},
    "order_items": {"item_id", "order_id", "product_id", "quantity", "unit_price"},
    "customers":   {"customer_id", "country", "signup_date", "segment"},
    "products":    {"product_id", "name", "category", "cost"},
}


# -------------------------
# SCHEMA VALIDATION (CSV)
# -------------------------

@task
def validate_csv_schema(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    expected = EXPECTED_SCHEMAS[table_name]
    actual   = set(df.columns.str.strip().str.lower())
    missing  = expected - actual
    extra    = actual - expected
    errors   = []
    if missing:
        errors.append(f"Missing columns: {missing}")
    if extra:
        errors.append(f"Unexpected columns: {extra}")
    if errors:
        raise ValueError(f"Schema validation FAILED for '{table_name}': {' | '.join(errors)}")
    print(f"Schema OK for '{table_name}': {sorted(actual)}")
    return df


# -------------------------
# MINIO EXTRACT
# -------------------------

@task
def extract_orders_from_minio() -> pd.DataFrame:
    obj = MINIO_CLIENT.get_object(BUCKET, "orders.csv")
    return pd.read_csv(BytesIO(obj.read()))


@task
def extract_order_items_from_minio() -> pd.DataFrame:
    obj = MINIO_CLIENT.get_object(BUCKET, "order_items.csv")
    return pd.read_csv(BytesIO(obj.read()))


# -------------------------
# POSTGRES EXTRACT
# -------------------------

@task
def extract_customers() -> pd.DataFrame:
    engine = create_engine(POSTGRES_CONN)
    return pd.read_sql("SELECT * FROM customers", engine)


@task
def extract_products() -> pd.DataFrame:
    engine = create_engine(POSTGRES_CONN)
    return pd.read_sql("SELECT * FROM products", engine)


# -------------------------
# CLEAN
# -------------------------

@task
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df.drop_duplicates()


# -------------------------
# DUCKDB LOAD
# -------------------------

@task
def load_to_duckdb(df: pd.DataFrame, table_name: str):
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    df = df.copy().astype("string")
    con.register("tmp", df)
    con.execute(f"CREATE OR REPLACE TABLE staging.{table_name} AS SELECT * FROM tmp")
    count = con.execute(f"SELECT COUNT(*) FROM staging.{table_name}").fetchone()[0]
    print(f"Loaded staging.{table_name}: {count} rows")
    con.close()


# -------------------------
# SODA VALIDATION
# -------------------------

@task
def run_soda_checks(checks_file: str, description: str):
    print(f"\nRunning Soda: {description}")
    scan = Scan()
    scan.set_data_source_name("duckdb")
    scan.add_configuration_yaml_file(SODA_CONFIG)
    scan.add_sodacl_yaml_file(checks_file)
    scan.set_verbose(False)
    scan.execute()
    if scan.has_check_fails():
        failed = [
            c.name for c in scan.get_checks()
            if str(c.outcome) == "CheckOutcome.fail"
        ]
        raise ValueError(f"Soda '{description}' FAILED — pipeline blocked.\nFailed: {failed}")
    print(f"Soda '{description}' passed.")


# -------------------------
# DBT
# -------------------------

@task
def dbt_run():
    print("\nRunning dbt run...")
    result = subprocess.run(
        [os.path.join(VENV_BIN, "dbt"), "run",
         "--project-dir", BASE_DIR,
         "--profiles-dir", DBT_PROFILES],
        capture_output=True, text=True, cwd=BASE_DIR
    )
    print(result.stdout)
    if result.returncode != 0:
        raise ValueError(f"dbt run FAILED:\n{result.stderr}")
    print("dbt run completed successfully.")


@task
def dbt_test():
    print("\nRunning dbt test...")
    result = subprocess.run(
        [os.path.join(VENV_BIN, "dbt"), "test",
         "--project-dir", BASE_DIR,
         "--profiles-dir", DBT_PROFILES],
        capture_output=True, text=True, cwd=BASE_DIR
    )
    print(result.stdout)
    if result.returncode != 0:
        raise ValueError(f"dbt test FAILED:\n{result.stderr}")
    print("dbt test completed successfully.")


# -------------------------
# STREAMLIT
# -------------------------

@task
def launch_streamlit():
    print("\nLaunching Streamlit dashboard...")
    subprocess.Popen(
        [os.path.join(VENV_BIN, "streamlit"), "run",
         os.path.join(BASE_DIR, "streamlit_app.py"),
         "--server.headless", "true"],
        cwd=BASE_DIR
    )
    print("Streamlit running at http://localhost:8501")


# -------------------------
# FULL PIPELINE FLOW
# -------------------------

@flow(log_prints=True)
def ingestion_flow():

    # STEP 1 — Extract
    orders      = extract_orders_from_minio()
    order_items = extract_order_items_from_minio()
    customers   = extract_customers()
    products    = extract_products()

    # STEP 2 — Validate CSV schema before loading
    orders      = validate_csv_schema(orders,      "orders")
    order_items = validate_csv_schema(order_items, "order_items")
    customers   = validate_csv_schema(customers,   "customers")
    products    = validate_csv_schema(products,    "products")

    # STEP 3 — Clean
    orders      = clean_dataframe(orders)
    order_items = clean_dataframe(order_items)
    customers   = clean_dataframe(customers)
    products    = clean_dataframe(products)

    # STEP 4 — Load into DuckDB staging
    load_to_duckdb(orders,      "orders")
    load_to_duckdb(order_items, "order_items")
    load_to_duckdb(customers,   "customers")
    load_to_duckdb(products,    "products")

    # STEP 5 — Soda: schema checks (columns + types)
    run_soda_checks(
        os.path.join(BASE_DIR, "soda", "checks_schema.yml"),
        "Schema validation (columns & types)"
    )

    # STEP 6 — Soda: quality checks (nulls, duplicates, values)
    run_soda_checks(
        os.path.join(BASE_DIR, "soda", "checks_staging.yml"),
        "Data quality staging (nulls, duplicates, accepted values)"
    )

    # STEP 7 — dbt run (staging → intermediate → marts)
    dbt_run()

    # STEP 8 — Soda: intermediate quality checks
    run_soda_checks(
        os.path.join(BASE_DIR, "soda", "checks_intermediate.yml"),
        "Data quality intermediate"
    )

    # STEP 9 — dbt test (all schema.yml tests)
    dbt_test()

    # STEP 10 — Launch Streamlit dashboard
    launch_streamlit()

    print("\nPipeline completed. Dashboard: http://localhost:8501")


# -------------------------
# ENTRYPOINT
# -------------------------

if __name__ == "__main__":
    ingestion_flow()
