CREATE TABLE IF NOT EXISTS bronze_files(
    file_name   String NOT NULL,
    downloaded_dt  Datetime DEFAULT now(),
    source_url  String NOT NULL,
    taxi_type_id  UInt8 DEFAULT NULL,
    processed   Boolean DEFAULT False,
    processed_dt    Datetime DEFAULT NULL
)
ENGINE = ReplacingMergeTree()
ORDER BY file_name
SETTINGS enable_block_number_column = 1,
    enable_block_offset_column = 1;
