from datetime import datetime
from pathlib import Path
import geopandas as gpd
import pandas as pd
import logging

pd.options.mode.chained_assignment = None

def impute_community_area(df):
    """
    Impute missing community_area values using GeoSpatial join with boundary file.
    Operates in EPSG:3435 (State Plane Illinois East NAD 1983).
    """
    try:
        missing_count = df["community_area"].isna().sum()
        if missing_count == 0:
            logging.info("No missing community_area values. Skipping spatial imputation.")
            return df

        logging.info(f"{missing_count} rows missing community_area. Performing spatial join...")

        # Load community area boundaries
        base_data_path = Path("../data")
        community_areas_file_name = "CommAreas_20250412.geojson"

        community_areas = gpd.read_file(base_data_path / community_areas_file_name)
        community_areas = community_areas.to_crs('EPSG:3435')
        community_areas.rename({'area_num_1': 'area_id'}, axis=1, inplace=True)
        community_areas['area_id'] = community_areas['area_id'].astype(int)

        # Convert to GeoDataFrame using X/Y coordinate
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['x_coordinate'], df['y_coordinate']))
        gdf = gdf.set_crs("EPSG:3435")

        # Perform spatial join
        joined = gpd.sjoin(gdf, community_areas, how="left", predicate='within')

        # Fill in missing community_area from spatial area_id
        imputed_values = joined['area_id'].notna() & df['community_area'].isna()
        df['community_area'] = df['community_area'].fillna(joined['area_id'])

        logging.info(f"Imputed community_area for {imputed_values.sum()} rows.")
    except Exception as e:
        logging.error("Error during spatial imputation.", exc_info=True)
        raise RuntimeError("Spatial imputation failed.") from e

    return df


def cast_column_types(
    df,
    numeric_int_cols,
    float_cols,
    string_cols,
    bool_cols,
    datetime_cols
):
    """
    Cast columns to target data types.
    Uses pandas nullable dtypes where appropriate.
    """

    try:

        for col in numeric_int_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        for col in string_cols:
            df[col] = df[col].astype("string")

        for col in bool_cols:
            df[col] = df[col].astype("boolean")

        for col in datetime_cols:
            df[col] = pd.to_datetime(df[col], errors="coerce")

        logging.info("Data type casting complete.")

    except Exception:
        logging.error("Failed during data type casting.", exc_info=True)
        raise RuntimeError("Data type casting failed.")

    return df

def clean_string_columns(df, string_cols):
    for col in string_cols:
        df[col] = df[col].str.strip().str.replace(r'\s+', ' ', regex=True).str.upper()

    return df


def drop_missing_data(df, drop_if_any=[], drop_if_all=[]):

    drop_if_any = drop_if_any or []
    drop_if_all = drop_if_all or []

    all_dropped = pd.DataFrame()

    if drop_if_any:
        mask = df[drop_if_any].isnull().any(axis=1)
        dropped = df[mask]
        df = df[~mask]
        all_dropped = pd.concat([all_dropped, dropped], ignore_index=True)
        logging.info(f"Dropped {len(dropped)} rows with ANY nulls in columns: {drop_if_any}")

    if drop_if_all:
        mask = df[drop_if_all].isnull().all(axis=1)
        dropped = df[mask]
        df = df[~mask]
        all_dropped = pd.concat([all_dropped, dropped], ignore_index=True)
        logging.info(f"Dropped {len(dropped)} rows with ALL nulls in columns: {drop_if_all}")

    if not all_dropped.empty:
        logging.info(f"Row count after dropping nulls: {len(df)}")
    else:
        logging.info("No rows dropped for null values.")
   
    return df, all_dropped

def drop_duplicates(df, subset_cols):
    before = len(df)
    is_duplicate = df.duplicated(subset=subset_cols, keep='last')
    dropped_rows = df[is_duplicate]
    df = df[~is_duplicate]

    if not dropped_rows.empty:
        logging.info(f"Dropped {before - len(df)} duplicate rows based on columns: {subset_cols}")
        logging.info(f"Row count after dropping duplicates: {len(df)}")
    else:
        logging.info("No duplicate rows found.")
    return df, dropped_rows

def drop_outliers(df, cols):
    # drop rows where x and y coordinates both are equal to 0
    mask = (df[cols] == 0).all(axis=1)
    dropped_rows = df[mask]
    df = df[~mask]
    logging.info(f"Dropped {mask.sum()} outlier rows based on columns: {cols}")
    return df, dropped_rows

def transform(df):
    logging.info("Starting transform step...")

    EXPECTED_COLUMNS = [
        "id", "case_number", "date", "block", "iucr", "primary_type", "description",
        "location_description", "arrest", "domestic", "beat", "district", "ward",
        "community_area", "fbi_code", "x_coordinate", "y_coordinate", "year",
        "updated_on", "latitude", "longitude"
    ]

    numeric_int_cols = ["id", "beat", "district", "ward", "community_area", "year"]
    float_cols = ["x_coordinate", "y_coordinate", "latitude", "longitude"]
    string_cols = [
        "case_number", "block", "iucr", "primary_type", "description",
        "location_description", "fbi_code"
    ]
    bool_cols = ["arrest", "domestic"]
    datetime_cols = ["date", "updated_on"]

    if df is None or df.empty:
        logging.error("No data provided to transform. Returning empty DataFrame.")
        raise ValueError("No data to transform.")

    initial_count = len(df)
    logging.info(f"Transforming raw data with {initial_count} rows.")

    # Add missing columns with None
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = None
            logging.warning(f"Column '{col}' missing. Added with None.")

    df = df[EXPECTED_COLUMNS]

    # Type Conversion
    df = cast_column_types(df, numeric_int_cols, float_cols, string_cols, bool_cols, datetime_cols)

    # Standardize String Columns
    df = clean_string_columns(df, string_cols)

    # Spatial Imputation of missing community_area
    df = impute_community_area(df)


    # Drop rows with missing values in specific columns
    drop_if_any_columns = [] # Let the downstream logic handle this for now
    drop_if_all_columns = [] # Let the downstream logic handle this for now
    df, dropped_nulls = drop_missing_data(df, drop_if_any_columns, drop_if_all_columns)

    # Drop duplicates based on specific columns
    subset_cols_for_duplicates = EXPECTED_COLUMNS[1:]  # Exclude 'id'
    df, dropped_duplicates = drop_duplicates(df, subset_cols_for_duplicates)

    # Drop outliers based on x and y coordinates
    subset_cols_for_outliers = ["x_coordinate", "y_coordinate"]
    df, dropped_outliers = drop_outliers(df, subset_cols_for_outliers)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    if not dropped_nulls.empty:
        dropped_nulls.to_csv(f"audit_missing_drops_{timestamp}.csv", index=False)
    if not dropped_duplicates.empty:
        dropped_duplicates.to_csv(f"audit_duplicate_drops_{timestamp}.csv", index=False)
    if not dropped_outliers.empty:
        dropped_outliers.to_csv(f"audit_outlier_drops_{timestamp}.csv", index=False)

    if df.empty:
        logging.warning("No records left after transformation.")
    else:
        logging.info(f"Transform complete. Final row count: {len(df)}")

    return df