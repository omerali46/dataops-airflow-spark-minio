import os
import io
import sys
import pandas as pd
import boto3
from sqlalchemy import create_engine

def need(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def main():
    minio_endpoint = need("MINIO_ENDPOINT")
    minio_bucket = need("MINIO_BUCKET")
    minio_user = need("MINIO_ROOT_USER")
    minio_pass = need("MINIO_ROOT_PASSWORD")

    pg_host = need("PG_HOST")
    pg_port = need("PG_PORT")
    pg_db = need("PG_DB")
    pg_user = need("PG_USER")
    pg_password = need("PG_PASSWORD")

    key = "raw/dirty_store_transactions.csv"
    print(f"[INFO] Reading s3://{minio_bucket}/{key} from {minio_endpoint}", flush=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_user,
        aws_secret_access_key=minio_pass,
        region_name="us-east-1",
    )

    obj = s3.get_object(Bucket=minio_bucket, Key=key)
    raw_bytes = obj["Body"].read()

    df = pd.read_csv(io.BytesIO(raw_bytes))
    print(f"[INFO] Loaded rows={len(df)} cols={len(df.columns)}", flush=True)

    # ---- Cleaning ----
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # drop fully empty rows
    df = df.dropna(how="all")

    # trim string columns
    for c in df.select_dtypes(include=["object"]).columns:
        df[c] = df[c].astype(str).str.strip()
        df.loc[df[c].isin(["", "nan", "None", "NULL", "null"]), c] = pd.NA

    # remove duplicates
    df = df.drop_duplicates()

    # example: try parse common date columns if exist
    for dc in ["date", "transaction_date", "created_at"]:
        if dc in df.columns:
            df[dc] = pd.to_datetime(df[dc], errors="coerce")

    print(f"[INFO] After cleaning rows={len(df)}", flush=True)

    # ---- Load to Postgres ----
    url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    engine = create_engine(url)

    table = "clean_data_transactions"
    print(f"[INFO] Writing to {pg_db}.public.{table} (replace)", flush=True)
    df.to_sql(table, engine, schema="public", if_exists="replace", index=False, chunksize=1000, method="multi")

    print("[INFO] DONE", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr, flush=True)
        raise
