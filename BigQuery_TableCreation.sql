CREATE TABLE IF NOT EXISTS `student_data.students` (
    StudentID INT64,
    StudentName STRING,
    Address STRING,
    Phone STRING,
    Admission_date DATE,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING
);


CREATE TABLE IF NOT EXISTS `student_data.file_load_log` (
    log_id STRING NOT NULL,
    file_name STRING NOT NULL,
    file_path STRING NOT NULL,
    last_modified_timestamp TIMESTAMP NOT NULL,
    file_size_bytes INT64,
    rows_processed INT64,
    load_status STRING,
    staging_file_path STRING,
    archive_file_path STRING,
    error_message STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    processed_by STRING
);