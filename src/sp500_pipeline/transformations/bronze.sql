-- =====================================================================
-- BRONZE LAYER
-- Raw, append-only copies of the landing files. No cleaning here:
-- types stay as landed, duplicates are expected (Silver dedupes).
-- Expectations only WARN so Bronze never loses data.
-- =====================================================================

-- Daily prices from yfinance
CREATE OR REFRESH STREAMING TABLE bronze_prices (
  CONSTRAINT ticker_not_null EXPECT (ticker IS NOT NULL),
  CONSTRAINT date_not_null   EXPECT (date IS NOT NULL),
  CONSTRAINT close_positive  EXPECT (close > 0)
)
COMMENT 'Raw daily S&P 500 prices from yfinance; append-only copy of landing files'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path AS source_file,      -- lineage: which file each row came from
  current_timestamp() AS bronze_loaded_at  -- when the row entered Bronze
FROM STREAM read_files(
  '/Volumes/workspace/sp500/landing/prices/',
  format      => 'parquet',
  schemaHints => 'date STRING, ticker STRING, open DOUBLE, high DOUBLE,
                  low DOUBLE, close DOUBLE, volume DOUBLE, ingested_at STRING'
);

-- S&P 500 constituents (ticker, company, sector) from Wikipedia
CREATE OR REFRESH STREAMING TABLE bronze_constituents (
  CONSTRAINT ticker_not_null EXPECT (ticker IS NOT NULL),
  CONSTRAINT sector_not_null EXPECT (sector IS NOT NULL)
)
COMMENT 'Raw S&P 500 constituent snapshots from Wikipedia; one snapshot per ingestion run'
TBLPROPERTIES ('quality' = 'bronze')
AS SELECT
  *,
  _metadata.file_path AS source_file,
  current_timestamp() AS bronze_loaded_at
FROM STREAM read_files(
  '/Volumes/workspace/sp500/landing/constituents/',
  format      => 'parquet',
  schemaHints => 'ticker STRING, company STRING, sector STRING,
                  sub_industry STRING, ingested_at STRING'
);