from datetime import datetime
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

def get_existing_ids(df, conn, table_name="crimes"):

    # TODO: Rewrite this function to handle the case when len(df) ~ millions of rows
    cur = conn.cursor()
    ids = tuple(df["id"].unique().tolist())
    if not ids:
        return set()

    query = f"""
    SELECT id FROM {table_name}
    WHERE id IN %s
    """
    cur.execute(query, (ids,))
    existing = set(row[0] for row in cur.fetchall())
    return existing

def chunk_dataframe(df, batch_size):
    """Yield DataFrame chunks of specified batch_size."""
    for start in range(0, len(df), batch_size):
        yield df.iloc[start:start + batch_size]

def prepare_for_postgres_insertion(df):
    """ Prepares DataFrame for psycopg2 insertion (convert pd.NA, NaN to None)."""
    # TODO: Make this function more efficient for large DataFrames
    return df.astype(object).where(pd.notnull(df), None)

def load(df, db_config, table_name="crimes", batch_size=50000):
    """
    Bulk insert or upsert data into PostgreSQL using psycopg2.
    Assumes 'id' is the primary key and updates all other columns on conflict.
    """
    if df.empty:
        logging.warning("No records to load. DataFrame is empty.")
        return

    logging.info(f"Loading {len(df)} records into '{table_name}' table.")
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Get column names and generate query
        columns = list(df.columns)
        col_names = ', '.join(columns)

        # Exclude 'id' for update clause
        update_columns = [col for col in columns if col != 'id']
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])

        existing_ids = get_existing_ids(df, conn, table_name)
        num_conflicts = len(existing_ids)
        total_records = len(df)
        num_inserts = total_records - num_conflicts

        logging.info(f"{num_conflicts} records will be updated (conflict).")
        logging.info(f"{num_inserts} records will be newly inserted.")

        query = f"""
        INSERT INTO {table_name} ({col_names})
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
        {update_clause};
        """

        df = prepare_for_postgres_insertion(df)
        # Prepare values
        batch_num = 1
        successful_batches = 0

        for chunk in chunk_dataframe(df, batch_size):
            values = [tuple(row) for row in chunk.to_numpy()]
            try:
                execute_values(cur, query, values)
                conn.commit()
                successful_batches += 1
                logging.info(f"Batch {batch_num}: Inserted {len(values)} rows.")
            except Exception as e:
                conn.rollback()
                timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
                chunk.to_csv(f"failed_batch_{batch_num}_{timestamp}.csv", index=False)
                logging.error(f"Batch {batch_num} failed. Rolled back.", exc_info=True)

            batch_num += 1

        cur.close()
        conn.close()

        total_batches = batch_num - 1

        if successful_batches == 0:
            logging.error("All batches failed. Load operation unsuccessful.")
            raise RuntimeError("All batches failed. Load operation unsuccessful.")
        elif successful_batches < total_batches:
            logging.error(f"Partial failure: {total_batches - successful_batches} out of {total_batches} batches failed.")
            raise RuntimeError(f"Partial failure: {total_batches - successful_batches} out of {total_batches} batches failed.")

        logging.info(f"Successfully upserted {total_records} records into '{table_name}'.")
        
    except Exception as e:
        logging.error("Failed to load data into the database.", exc_info=True)
        raise RuntimeError("Failed to load data into the database.") from e