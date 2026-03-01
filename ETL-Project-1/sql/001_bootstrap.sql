CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS silver.pipeline_audit (
  run_id VARCHAR(128),
  dataset VARCHAR(64),
  source_count BIGINT,
  loaded_at TIMESTAMP DEFAULT GETDATE(),
  status VARCHAR(64),
  details VARCHAR(65535)
);

-- Dataset tables (silver.songs, silver.users, silver.streams) are auto-created by loader.
-- Gold reporting tables are auto-created by pipeline after silver loads.
