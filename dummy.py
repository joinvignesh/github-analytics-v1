import os
import requests
import pandas as pd
import json
import concurrent.futures
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
from typing import List, Tuple, Optional

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
# 1. ROBUST SESSION (Prevents Connection Drops)
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
            
    # Handle NaNs and convert timestamps to string if necessary (Snowflake handles ISO strings well)
    return df.where(pd.notnull(df), None)

def get_max_updated_at(engine, table_name: str, owner: str, repo: str) -> Optional[str]:
    """Checks Snowflake for the last ingested timestamp to enable incremental loading."""
    try:
        # Check if table exists first
        query_check = text(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'RAW' AND TABLE_NAME = '{table_name}'")
        with engine.connect() as conn:
            exists = conn.execute(query_check).scalar()
            
            if not exists:
                return None

            # Get max updated_at
            query_date = text(f"""
                SELECT MAX(updated_at) 
                FROM RAW.{table_name} 
                WHERE SOURCE_OWNER = :owner AND SOURCE_REPO = :repo
            """)
            result = conn.execute(query_date, {"owner": owner, "repo": repo}).scalar()
            
            if result:
                # GitHub expects ISO 8601. Ensure it's formatted correctly.
                # If result is datetime object, convert to string
                if isinstance(result, datetime):
                    return result.isoformat()
                return str(result)
            return None
    except Exception as e:
        print(f"Warning: Could not fetch max date for {table_name}: {e}")
        return None

def fetch_pages(session, url, params):
    """Generator that yields pages of data."""
    while url:
        try:
            response = session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data:
                break
                
            yield data
            
            # Get next page URL
            url = response.links.get("next", {}).get("url")
            params = None # Clear params after first request as they are in the 'next' URL
            
        except requests.exceptions.RequestException as e:
            print(f"Error requesting {url}: {e}")
            raise e

# ----------------------------------------------------
# 3. CORE INGESTION LOGIC
# ----------------------------------------------------
def ingest_resource(engine, owner: str, repo: str, resource_type: str):
    """
    Generic function to ingest Issues or Comments.
    resource_type: 'issues' or 'issues/comments'
    """
    session = get_github_session()
    
    # Determine table name
    if resource_type == "issues":
        table_name = "GITHUB_ISSUES"
        endpoint = f"/repos/{owner}/{repo}/issues"
    elif resource_type == "issues/comments":
        table_name = "GITHUB_COMMENTS"
        endpoint = f"/repos/{owner}/{repo}/issues/comments"
    else:
        raise ValueError("Unknown resource type")

    # 1. INCREMENTAL CHECK
    since_date = get_max_updated_at(engine, table_name, owner, repo)
    params = {"per_page": 100, "state": "all"}
    
    mode_msg = "Full Load"
    if since_date:
        params["since"] = since_date
        mode_msg = f"Incremental Load (since {since_date})"

    print(f"[{owner}/{repo}] Starting {resource_type}... Mode: {mode_msg}")

    total_ingested = 0
    url = f"{BASE_URL}{endpoint}"
    
    # 2. BATCH FETCH AND INSERT
    # We fetch one page, process it, insert it. This keeps memory usage low.
    for page_data in fetch_pages(session, url, params):
        if not page_data:
            continue
            
        df = pd.json_normalize(page_data)
        
        # Add Metadata
        df["SOURCE_OWNER"] = owner
        df["SOURCE_REPO"] = repo
        df["ingested_at"] = datetime.now(timezone.utc)
        
        # Clean
        df = clean_df_for_snowflake(df)
        
        # Insert
        # Note: We use 'append' for both initial and incremental. 
        # For incremental, Snowflake might have duplicates if records updated. 
        # Standard pattern is to append to RAW, and deduplicate in dbt later.
        df.to_sql(table_name, engine, schema="RAW", if_exists="append", index=False, chunksize=5000)
        
        total_ingested += len(df)
        print(f"[{owner}/{repo}] {resource_type}: Ingested batch of {len(df)} records...")

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
        
        # Always replace repo metadata to keep it fresh
        with engine.connect() as conn:
            conn.execute(text('DROP TABLE IF EXISTS "RAW"."GITHUB_REPOSITORIES"'))
            conn.commit()
            
        df.to_sql("GITHUB_REPOSITORIES", engine, schema="RAW", if_exists="append", index=False)
        print("Repository metadata updated.")

# ----------------------------------------------------
# 4. PARALLEL EXECUTION MAIN
# ----------------------------------------------------
def main():
    if not GITHUB_TOKEN: raise ValueError("Missing GITHUB_TOKEN")
    
    # Use connection pooling in SQLAlchemy
    engine = create_engine(WAREHOUSE_CONN_STRING, pool_size=10, max_overflow=20)
    
    start_time = datetime.now()
    print("Starting Optimized GitHub Ingestion...")

    # 1. Update Repo Metadata (Fast, sequential is fine)
    ingest_repos_metadata(engine)

    # 2. Parallel Ingestion for heavy data
    # We will spin up tasks: 2 Repos * 2 Resources (Issues + Comments) = 4 Parallel Threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        
        for owner, repo in REPOSITORIES:
            # Task 1: Issues
            futures.append(executor.submit(ingest_resource, engine, owner, repo, "issues"))
            # Task 2: Comments
            futures.append(executor.submit(ingest_resource, engine, owner, repo, "issues/comments"))

        # Wait for all to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"A thread crashed: {e}")

    duration = datetime.now() - start_time
    print(f"All jobs completed in {duration}.")

if __name__ == "__main__":
    main()