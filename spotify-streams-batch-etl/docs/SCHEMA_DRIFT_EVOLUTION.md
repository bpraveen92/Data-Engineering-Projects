# Schema Drift & Evolution

The pipeline handles schema changes by detecting new columns, tracking schemas in S3, and dynamically adding columns to Redshift—all without manual intervention.

**Handled scenarios:**
- ✅ New columns arrive (auto-detected and added)
- ✅ Columns removed (kept as NULLs for backward compatibility)
- ✅ Type changes (cast to string in transform layer)
- ✅ Column renames (treated as new + old columns)
- ❌ Required column deletion (caught by quality checks)

---

## Stage 1: CSV Reading & Normalization

**File**: `transform_jobs/csv_to_curated_transform_job.py`

**Input CSV (Run 1):**
```
track_id,track_name,artists,duration_ms,popularity
SONG001,Track One,Artist A,180000,75
SONG002,Track Two,Artist B,240000,85
```

**Input CSV (Run 2 - with new "genre" column):**
```
track_id,track_name,artists,duration_ms,popularity,genre
SONG001,Track One,Artist A,180000,75,Rock
SONG003,Track Three,Artist C,200000,90,Pop
```

**How it's handled:**
- Pandas auto-detects all columns, including new `genre`
- `clean_column_name()` normalizes all names to snake_case: "Track ID" → "track_id", "Genre" → "genre"
- Each row tagged with `source_file` for audit trail
- All columns cast to string for type-agnostic processing

---

## Stage 2: Dataset Transformations & Type Casting

**File**: `transform_jobs/csv_to_curated_transform_job.py`

**Design principle:** Only transform columns that exist; new columns pass through unchanged.

```python
def transform_songs(df):
    # Apply only to columns that exist (column-aware)
    if "duration_ms" in df.columns:
        df["duration_ms"] = (df["duration_ms"] / 1000).round(1)
    if "popularity" in df.columns:
        df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce")
    # "genre" column (if present) is unchanged
    return df

def cast_all_to_string(df):
    # Cast everything to string for schema agnosticism
    for col in df.columns:
        df[col] = df[col].astype(str)
    return df
```

**Result:**
- All columns become strings regardless of source type
- Type incompatibilities (e.g., popularity becomes "Very High") are handled downstream
- New columns don't break existing transformation logic

---

## Stage 3: Schema Drift Detection

**File**: `transform_jobs/csv_to_curated_transform_job.py`

**S3 Schema Snapshot:**
```json
{
  "dataset": "songs",
  "updated_at_utc": "2026-03-01T13:00:00+00:00",
  "columns": ["artists", "duration_ms", "popularity", "track_id", "track_name"]
}
```

**Run 2 - Schema Evolution:**
```python
def align_to_schema(df, existing_columns):
    """Detect new columns and align incoming data"""
    METADATA_COLUMNS = ["run_id", "ingest_ts", "ingest_date", "source_file"]
    incoming_columns = [c for c in df.columns if c not in METADATA_COLUMNS]
    
    # Union sets to discover new columns
    final_columns = sorted(set(existing_columns).union(incoming_columns))
    
    # existing_columns: {artists, duration_ms, popularity, track_id, track_name}
    # incoming_columns: {artists, duration_ms, genre, popularity, track_id, track_name}
    # final_columns:   {artists, duration_ms, genre, popularity, track_id, track_name} ← genre added!
    
    # Add missing columns as NULLs
    for col in final_columns:
        if col not in df.columns:
            df[col] = None
    
    return df[final_columns + METADATA_COLUMNS], final_columns
```

**Design advantage:** Union approach allows schemas to grow. If "popularity" stops arriving, it remains in schema as NULL—no breaking changes.

---

## Stage 4: Validation & Quality Checks

**File**: `src/quality/ge_validator.py`

**Config** (`config/quality_rules.yaml`):
```yaml
datasets:
  songs:
    required_columns: [track_id, track_name, artists]
    unique_columns: [track_id]
    max_null_ratio:
      track_name: 0.05
```

**How it protects schema:**
- Missing required columns → DAG fails + operator alerted
- New non-required columns (e.g., "genre") → quality check passes
- Quality rules are flexible, not brittle

---

## Stage 5: Loading to Redshift

**File**: `src/loaders/redshift_loader.py`

**Design pattern:** Read schema from S3, dynamically add missing columns, load with explicit column list.

```python
def load_curated_partition_to_redshift(run_id, dataset):
    # 1. Load schema from S3
    schema_columns = load_schema_columns_from_s3(curated_bucket, dataset)
    # Returns: ["artists", "duration_ms", "genre", "popularity", "track_id", "track_name"]
    
    # 2. Ensure table exists
    ensure_tables_exist(session, "silver.songs")
    
    # 3. Add any NEW columns dynamically
    existing = get_existing_columns(session, "silver.songs")
    for col in schema_columns:
        if col not in existing:
            session.execute(f"ALTER TABLE silver.songs ADD COLUMN {col} VARCHAR(65535);")
    
    # 4. Build explicit column list
    column_list = ", ".join(schema_columns + ["run_id", "ingest_ts", "ingest_date", "source_file"])
    
    # 5. Load with COPY
    copy_sql = f"""
    COPY silver.songs_staging ({column_list})
    FROM 's3://bucket/curated/music/songs/run_id=20260301_130510/'
    IAM_ROLE 'arn:aws:iam::...'
    FORMAT AS PARQUET;
    """
    session.execute(copy_sql)
    
    # 6. Upsert (delete duplicates, insert new rows)
    session.execute(f"""
    DELETE FROM silver.songs USING silver.songs_staging 
    WHERE silver.songs.track_id = silver.songs_staging.track_id;
    
    INSERT INTO silver.songs SELECT * FROM silver.songs_staging;
    """)
```

**Why this works:**
- `ALTER TABLE ADD COLUMN` is fast and non-blocking in modern Redshift
- Explicit column list prevents schema mismatches
- INSERT...SELECT automatically includes "genre" column in upsert

---

## Example Flow: New "Genre" Column

**Run 1 - Initial State:**
```
CSV Input:          [track_id, track_name, artists, duration_ms, popularity]
After Transform:    [track_id, track_name, artists, duration_ms, popularity] (all strings)
Redshift Table:     5 columns
Schema in S3:       ["artists", "duration_ms", "popularity", "track_id", "track_name"]
```

**Run 2 - Genre Column Added:**
```
CSV Input:          [track_id, track_name, artists, duration_ms, popularity, genre] ← NEW
After Transform:    [track_id, track_name, artists, duration_ms, popularity, genre] (all strings)
Schema Detection:   Union finds "genre" is new
Redshift Actions:   
  1. ALTER TABLE silver.songs ADD COLUMN genre VARCHAR(65535);
  2. INSERT with genre values populated
Schema in S3:       ["artists", "duration_ms", "genre", "popularity", "track_id", "track_name"]
Result:             
  - Old rows (SONG001, SONG002): genre = NULL
  - New rows (SONG003, SONG004): genre = "Pop", "Jazz"
```

**Run 3 - Column Disappears:**
```
CSV Input:          [track_id, track_name, artists, duration_ms] ← popularity removed
After Transform:    [track_id, track_name, artists, duration_ms] 
Schema Detection:   Union keeps popularity (not removed, just absent in this batch)
DataFrame:          Adds popularity = NULL for new rows
Redshift Actions:   No ALTER TABLE needed
Schema in S3:       Unchanged (still has popularity)
Result:             New rows have popularity = NULL (backward compatible)
```

---

## Summary

| Scenario | Handling | Result |
|----------|----------|--------|
| **New column arrives** | Pandas detects → align_to_schema() unions → ALTER TABLE ADD COLUMN | Column auto-added to Redshift |
| **Column removed** | Union keeps old columns → filled with NULL | Backward compatible; no breaking changes |
| **Type changes** | Cast all to string in transform layer | Type-agnostic; handled by downstream SQL |
| **Required column missing** | Quality checks detect → DAG fails | Explicit failure with operator alert |

**Key design patterns:**
- Union set logic for schema evolution (grows, never shrinks)
- String casting for type tolerance
- Column-aware transformations (don't assume columns exist)
- Explicit column lists in Redshift COPY statements
- S3 schema snapshots as single source of truth
