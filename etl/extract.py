import time, logging
from datetime import datetime
from sodapy import Socrata
import psycopg2
from requests.exceptions import Timeout


import time, logging
from datetime import datetime
from sodapy import Socrata
import psycopg2
from requests.exceptions import Timeout

def extract(
    db_config,
    app_token=None,
    dataset_id="ijzp-q8t2",
    api_domain="data.cityofchicago.org",
    batch_size=1000,
    max_retries=5,
    request_timeout=10
):
    logging.info("Starting extract step...")

    # Get latest updated_on from DB
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        cur.execute("SELECT MAX(updated_on) FROM crimes;")
        latest_updated = cur.fetchone()[0]
        formatted_time = latest_updated.isoformat()
        cur.close()
        conn.close()
        logging.info(f"Max 'updated_on' from DB: {formatted_time}")
    except Exception:
        logging.error("Failed to fetch latest updated_on", exc_info=True)
        raise RuntimeError("Failed to fetch latest updated_on from DB.")

    # Initialize Socrata client
    try:
        client = Socrata(api_domain, app_token, timeout=request_timeout)
    except Exception:
        logging.error("Failed to create Socrata client", exc_info=True)
        raise RuntimeError("Failed to initialize Socrata client.")

    # Define a retryable fetch function
    def fetch_with_retry(offset):
        for attempt in range(max_retries):
            try:
                results = client.get(
                    dataset_id,
                    where=f"updated_on >= '{formatted_time}'",
                    limit=batch_size,
                    offset=offset
                )
                return results
            except Timeout as e:
                backoff_time = 2 ** attempt
                logging.warning(f"API Timeout (attempt {attempt + 1}): {str(e)}. Retrying in {backoff_time}s...")
                time.sleep(backoff_time)
            except Exception as e:
                backoff_time = 2 ** attempt
                logging.warning(f"API error (attempt {attempt + 1}): {str(e)}. Retrying in {backoff_time}s...")
                time.sleep(backoff_time)
        raise RuntimeError(f"API retries exhausted at offset {offset}")

    # Paginate and collect results
    all_records = []
    offset = 0

    while True:
        results = fetch_with_retry(offset)
        if not results:
            logging.info("No more new/updated records found.")
            break

        logging.info(f"Fetched {len(results)} records in batch {offset // batch_size}.")

        all_records.extend(results)
        offset += batch_size


    if not all_records:
        raise ValueError("Extract step completed but returned no data.")
    
    logging.info(f"Fetched {len(all_records)} new/updated records.")
    logging.info("Extract step completed successfully.")

    return all_records
