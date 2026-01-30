import os
import requests
import pandas as pd
import json
import concurrent.futures
from datetime import datetime, timezone

# 1. Import SQLAlchemy types explicitly
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine, Connection 
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from typing import List, Tuple, Optional

# ----------------------------------------------------
# MONKEYPATCH: Force Pandas to recognize SQLAlchemy 1.4
# ----------------------------------------------------
import pandas.io.sql
import pandas.io.common

def is_sqlalchemy_connectable_patched(con):
    return isinstance(con, (Engine, Connection))

pandas.io.common.is_sqlalchemy_connectable = is_sqlalchemy_connectable_patched
pandas.io.sql.is_sqlalchemy_connectable = is_sqlalchemy_connectable_patched
# ----------------------------------------------------

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
WAREHOUSE_CONN_STRING = os.getenv("WAREHOUSE_CONN_STRING")

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

REPOSITORIES: List[Tuple[str, str]] = [
    ("apache", "airflow"),
    ("dbt-labs", "dbt-core")
]

BASE_URL = "https://api.github.com"

# ----------------------------------------------------
# 1. ROBUST SESSION
# ----------------------------------------------------
def get_github_session():
    session = requests.Session()
    retry = Retry(
        total=5, 
        backoff_factor=1, 
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session

# ----------------------------------------------------
# 2. HELPERS
# ----------------------------------------------------
def clean_df_for_snowflake(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    
    # Standardize headers
    df.columns = [c.replace(".", "_").upper() for c in df.columns]
    
    # Serialize JSON objects to strings
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(
                lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
            )
            
    return df.where(pd.notnull(df), None)

def align_df_to_table(engine, df: pd.DataFrame, table_name: str, schema="RAW") -> pd.DataFrame:
    """
    Drops columns from the DataFrame that do not exist in the Snowflake table.
    Prevents 'invalid identifier' errors during incremental loads.
    """
    try:
        inspector = inspect(engine)
        # Check if table exists (case insensitive check)
        table_exists = False
        if inspector.has_table(table_name.lower(), schema=schema.lower()) or \
           inspector.has_table(table_name.upper(), schema=schema.upper()):
            table_exists = True

        if not table_exists:
            return df

        # Get columns from Snowflake (normalize to uppercase)
        db_columns = [col['name'].upper() for col in inspector.get_columns(table_name, schema=schema)]
        
        if not db_columns:
            return df
            
        # Keep only columns that exist in DB
        available_cols = [c for c in df.columns if c in db_columns]
        
        dropped_cols = set(df.columns) - set(available_cols)
        if dropped_cols:
            print(f"[{table_name}] ⚠️  Dropping new API columns to match existing table schema: {dropped_cols}")
            
        return df[available_cols]
        
    except Exception as e:
        print(f"Warning: Could not align schema for {table_name}: {e}")
        return df

def get_max_updated_at(engine, table_name: str, owner: str, repo: str) -> Optional[str]:
    try:
        query_check = text(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'RAW' AND TABLE_NAME = '{table_name}'")
        with engine.connect() as conn:
            exists = conn.execute(query_check).scalar()
            
            if not exists:
                return None

            query_date = text(f"""
                SELECT MAX(updated_at) 
                FROM RAW.{table_name} 
                WHERE SOURCE_OWNER = :owner AND SOURCE_REPO = :repo
            """)
            result = conn.execute(query_date, {"owner": owner, "repo": repo}).scalar()
            
            if result:
                if isinstance(result, datetime):
                    return result.isoformat()
                return str(result)
            return None
    except Exception as e:
        print(f"Warning: Could not fetch max date for {table_name}: {e}")
        return None

def fetch_pages(session, url, params):
    while url:
        try:
            response = session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                break
                
            yield data
            
            url = response.links.get("next", {}).get("url")
            params = None 
            
        except requests.exceptions.RequestException as e:
            print(f"Error requesting {url}: {e}")
            raise e

# ----------------------------------------------------
# 3. CORE INGESTION LOGIC
# ----------------------------------------------------
def ingest_resource(engine, owner: str, repo: str, resource_type: str):
    session = get_github_session()
    
    if resource_type == "issues":
        table_name = "GITHUB_ISSUES"
        endpoint = f"/repos/{owner}/{repo}/issues"
    elif resource_type == "issues/comments":
        table_name = "GITHUB_COMMENTS"
        endpoint = f"/repos/{owner}/{repo}/issues/comments"
    else:
        raise ValueError("Unknown resource type")

    since_date = get_max_updated_at(engine, table_name, owner, repo)
    params = {"per_page": 100, "state": "all"}
    
    mode_msg = "Full Load"
    if since_date:
        params["since"] = since_date
        mode_msg = f"Incremental Load (since {since_date})"

    print(f"[{owner}/{repo}] Starting {resource_type}... Mode: {mode_msg}")

    total_ingested = 0
    url = f"{BASE_URL}{endpoint}"
    
    for page_data in fetch_pages(session, url, params):
        if not page_data:
            continue
            
        df = pd.json_normalize(page_data)
        
        df["SOURCE_OWNER"] = owner
        df["SOURCE_REPO"] = repo
        df["ingested_at"] = datetime.now(timezone.utc)
        
        df = clean_df_for_snowflake(df)
        
        # Prevent Schema Drift Errors
        df = align_df_to_table(engine, df, table_name)
        
        try:
            df.to_sql(
                table_name, 
                con=engine, 
                schema="RAW", 
                if_exists="append", 
                index=False, 
                chunksize=5000,
                method="multi"
            )
            total_ingested += len(df)
            print(f"[{owner}/{repo}] {resource_type}: Ingested batch of {len(df)} records...")
        except Exception as e:
            print(f"Error inserting batch for {owner}/{repo}: {e}")
            raise e

    print(f"[{owner}/{repo}] Finished {resource_type}. Total: {total_ingested}")

def ingest_repos_metadata(engine):
    """Ingests repository metadata (Small data, single call)"""
    session = get_github_session()
    records = []
    
    for owner, repo in REPOSITORIES:
        try:
            resp = session.get(f"{BASE_URL}/repos/{owner}/{repo}")
            resp.raise_for_status()
            records.append(resp.json())
        except Exception as e:
            print(f"Error fetching repo metadata {owner}/{repo}: {e}")

    if records:
        df = pd.json_normalize(records)
        df["ingested_at"] = datetime.now(timezone.utc)
        df = clean_df_for_snowflake(df)
        
        # FIX: Manually drop the table to bypass Pandas reflection error
        with engine.begin() as conn:
            conn.execute(text('DROP TABLE IF EXISTS "RAW"."GITHUB_REPOSITORIES"'))
        
        # Use 'append' instead of 'replace'. Pandas will create the table since we dropped it.
        df.to_sql(
            "GITHUB_REPOSITORIES", 
            con=engine, 
            schema="RAW", 
            if_exists="append", 
            index=False,
            method="multi"
        )
        print("Repository metadata updated.")

# ----------------------------------------------------
# 4. PARALLEL EXECUTION MAIN
# ----------------------------------------------------
def main():
    if not GITHUB_TOKEN: raise ValueError("Missing GITHUB_TOKEN")
    
    engine = create_engine(WAREHOUSE_CONN_STRING, pool_size=10, max_overflow=20)
    
    start_time = datetime.now()
    print("Starting Optimized GitHub Ingestion...")

    # 1. Run Metadata Ingestion (Sequential)
    ingest_repos_metadata(engine)

    # 2. Run Heavy Data Ingestion (Parallel)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for owner, repo in REPOSITORIES:
            futures.append(executor.submit(ingest_resource, engine, owner, repo, "issues"))
            futures.append(executor.submit(ingest_resource, engine, owner, repo, "issues/comments"))

        # Check for exceptions in threads
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"A thread crashed: {e}")
                # Re-raise to ensure Airflow marks the task as FAILED
                raise e

    duration = datetime.now() - start_time
    print(f"All jobs completed in {duration}.")

if __name__ == "__main__":
    main()