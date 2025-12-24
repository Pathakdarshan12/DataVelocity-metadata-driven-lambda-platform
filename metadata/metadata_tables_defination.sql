-- =====================================================
-- METADATA TABLES
-- =====================================================
-- 1. DATA_FILE_FORMAT: Defines file formats and their characteristics
CREATE OR REPLACE TABLE SWIGGY.COMMON.DATA_FILE_FORMAT (
    file_format_id INTEGER AUTOINCREMENT PRIMARY KEY,
    file_format_name VARCHAR(100) NOT NULL UNIQUE,
    file_format_type VARCHAR(20) NOT NULL, -- CSV, JSON, PARQUET, AVRO, XML
    description VARCHAR(500),
    source_system VARCHAR(100),
    s3_bucket_path VARCHAR(500),
    file_name_pattern VARCHAR(200),

    -- File format specifications
    delimiter VARCHAR(10),
    has_header BOOLEAN DEFAULT TRUE,
    skip_header INTEGER DEFAULT 1,
    field_optionally_enclosed_by VARCHAR(10),
    escape_character VARCHAR(10),
    null_if ARRAY,
    encoding VARCHAR(20) DEFAULT 'UTF-8',
    compression VARCHAR(20),

    -- Target table info
    target_schema VARCHAR(100),
    target_table VARCHAR(100),

    -- Processing config
    is_active BOOLEAN DEFAULT TRUE,
    load_frequency VARCHAR(50),
    incremental_key VARCHAR(100),

    -- Audit
    created_by VARCHAR(100) DEFAULT CURRENT_USER(),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_by VARCHAR(100),
    updated_at TIMESTAMP_NTZ
)
COMMENT = 'Master table defining file formats, sources, and processing configurations';

-- 2. DATA_FIELD: Defines fields/columns in source files
CREATE OR REPLACE TABLE SWIGGY.COMMON.data_field (
    data_field_id INTEGER AUTOINCREMENT PRIMARY KEY,
    file_format_id INTEGER NOT NULL,
    field_name VARCHAR(200) NOT NULL, -- source field name
    field_position INTEGER, -- for positional files
    field_data_type VARCHAR(50) NOT NULL, -- STRING, INTEGER, DECIMAL, DATE, TIMESTAMP, BOOLEAN
    field_length INTEGER,
    field_precision INTEGER,
    field_scale INTEGER,
    is_nullable BOOLEAN DEFAULT TRUE,
    default_value VARCHAR(500),
    description VARCHAR(500),

    -- JSON specific
    json_path VARCHAR(500), -- e.g., '$.customer.address.city'
    is_nested BOOLEAN DEFAULT FALSE,

    -- Validation rules
    validation_rules VARIANT, -- JSON object with validation rules
    business_rules VARCHAR(1000),

    -- Audit columns
    created_by VARCHAR(100) DEFAULT CURRENT_USER(),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_by VARCHAR(100),
    updated_at TIMESTAMP_NTZ,

    CONSTRAINT fk_data_field_format FOREIGN KEY (file_format_id)
        REFERENCES SWIGGY.COMMON.data_file_format(file_format_id),
    CONSTRAINT uq_field_per_format UNIQUE (file_format_id, field_name)
)
COMMENT = 'Defines individual fields/columns within each file format';

-- 3. DATABASE_COLUMN_MAPPING: Maps source fields to target database columns
CREATE OR REPLACE TABLE SWIGGY.COMMON.database_column_mapping (
    mapping_id INTEGER AUTOINCREMENT PRIMARY KEY,
    data_field_id INTEGER NOT NULL,

    -- Target column information
    target_schema VARCHAR(100) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    target_column VARCHAR(200) NOT NULL,
    target_data_type VARCHAR(50) NOT NULL,

    -- Transformation logic
    transformation_logic VARCHAR(2000), -- SQL expression or function
    transformation_type VARCHAR(50), -- DIRECT, CALCULATED, LOOKUP, DERIVED
    is_primary_key BOOLEAN DEFAULT FALSE,
    is_business_key BOOLEAN DEFAULT FALSE,
    is_required BOOLEAN DEFAULT FALSE,

    -- SCD Type 2 specific
    is_scd_tracked BOOLEAN DEFAULT FALSE, -- track changes for this field

    -- Data quality
    data_quality_rules VARIANT, -- JSON with DQ rules

    -- Mapping metadata
    mapping_priority INTEGER DEFAULT 1, -- order of execution
    is_active BOOLEAN DEFAULT TRUE,
    effective_from_date DATE DEFAULT CURRENT_DATE(),
    effective_to_date DATE,

    -- Audit columns
    created_by VARCHAR(100) DEFAULT CURRENT_USER(),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_by VARCHAR(100),
    updated_at TIMESTAMP_NTZ,

    CONSTRAINT fk_mapping_data_field FOREIGN KEY (data_field_id)
        REFERENCES SWIGGY.COMMON.data_field(data_field_id),
    CONSTRAINT uq_target_column UNIQUE (data_field_id, target_schema, target_table, target_column)
)
COMMENT = 'Maps source fields to target database columns with transformation logic';

-- 4. DATA_FILE_DATA_FORMAT: Junction table for many-to-many relationships
-- (if a file format can map to multiple data formats or vice versa)
CREATE OR REPLACE TABLE SWIGGY.COMMON.data_file_data_format (
    file_data_format_id INTEGER AUTOINCREMENT PRIMARY KEY,
    file_format_id INTEGER NOT NULL,
    data_field_id INTEGER NOT NULL,

    -- Override configurations for this specific relationship
    field_order INTEGER, -- display or processing order
    is_required_in_file BOOLEAN DEFAULT TRUE,
    is_included_in_load BOOLEAN DEFAULT TRUE,

    -- Conditional logic
    include_condition VARCHAR(500), -- SQL condition when to include this field
    exclude_condition VARCHAR(500),

    -- Audit columns
    created_by VARCHAR(100) DEFAULT CURRENT_USER(),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT fk_file_data_format_file FOREIGN KEY (file_format_id)
        REFERENCES SWIGGY.COMMON.data_file_format(file_format_id),
    CONSTRAINT fk_file_data_format_field FOREIGN KEY (data_field_id)
        REFERENCES SWIGGY.COMMON.data_field(data_field_id),
    CONSTRAINT uq_file_field_combo UNIQUE (file_format_id, data_field_id)
)
COMMENT = 'Junction table for flexible file format to field relationships';

-- =====================================================
-- SUPPORTING METADATA TABLES
-- =====================================================

-- 5. FILE_LOAD_HISTORY: Track file processing history
CREATE OR REPLACE TABLE SWIGGY.COMMON.file_load_history (
    load_id INTEGER AUTOINCREMENT PRIMARY KEY,
    file_format_id INTEGER NOT NULL,
    file_name VARCHAR(500) NOT NULL,
    file_path VARCHAR(1000) NOT NULL,
    file_size_bytes INTEGER,
    file_row_count INTEGER,

    -- Load execution details
    load_status VARCHAR(50), -- STARTED, COMPLETED, FAILED, PARTIAL
    load_start_time TIMESTAMP_NTZ,
    load_end_time TIMESTAMP_NTZ,
    rows_processed INTEGER,
    rows_inserted INTEGER,
    rows_updated INTEGER,
    rows_rejected INTEGER,
    error_message VARCHAR(5000),

    -- Audit
    loaded_by VARCHAR(100) DEFAULT CURRENT_USER(),
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT fk_load_history_format FOREIGN KEY (file_format_id)
        REFERENCES SWIGGY.COMMON.data_file_format(file_format_id)
)
COMMENT = 'Audit trail of all file processing activities';;

-- 6. DATA_QUALITY_RULES: Centralized data quality rules
CREATE OR REPLACE TABLE SWIGGY.COMMON.data_quality_rules (
    rule_id INTEGER AUTOINCREMENT PRIMARY KEY,
    rule_name VARCHAR(200) NOT NULL UNIQUE,
    rule_type VARCHAR(50) NOT NULL, -- RANGE, PATTERN, LOOKUP, CUSTOM
    rule_category VARCHAR(50), -- COMPLETENESS, ACCURACY, CONSISTENCY, VALIDITY
    rule_expression VARCHAR(2000) NOT NULL,
    rule_description VARCHAR(1000),
    severity VARCHAR(20) DEFAULT 'ERROR', -- ERROR, WARNING, INFO
    is_active BOOLEAN DEFAULT TRUE,

    created_by VARCHAR(100) DEFAULT CURRENT_USER(),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Centralized repository of data quality validation rules';;

-- =====================================================
-- VIEWS FOR EASY QUERYING
-- =====================================================

-- Complete mapping view: file format -> fields -> target columns
CREATE OR REPLACE VIEW SWIGGY.COMMON.vw_complete_field_mapping AS
SELECT
    ff.file_format_id,
    ff.file_format_name,
    ff.file_format_type,
    ff.source_system,
    ff.s3_bucket_path,
    ff.target_schema AS file_target_schema,
    ff.target_table AS file_target_table,

    df.data_field_id,
    df.field_name AS source_field_name,
    df.field_position,
    df.field_data_type AS source_data_type,
    df.json_path,

    dcm.mapping_id,
    dcm.target_schema,
    dcm.target_table,
    dcm.target_column,
    dcm.target_data_type,
    dcm.transformation_logic,
    dcm.transformation_type,
    dcm.is_primary_key,
    dcm.is_business_key,
    dcm.is_scd_tracked,
    dcm.mapping_priority,

    ff.is_active AS format_active,
    dcm.is_active AS mapping_active

FROM SWIGGY.COMMON.data_file_format ff
INNER JOIN SWIGGY.COMMON.data_field df
    ON ff.file_format_id = df.file_format_id
INNER JOIN SWIGGY.COMMON.database_column_mapping dcm
    ON df.data_field_id = dcm.data_field_id
WHERE ff.is_active = TRUE
  AND dcm.is_active = TRUE
ORDER BY ff.file_format_name, df.field_position, dcm.mapping_priority;

-- View for file processing configuration
CREATE OR REPLACE VIEW SWIGGY.COMMON.vw_file_processing_config AS
SELECT
    ff.file_format_id,
    ff.file_format_name,
    ff.file_format_type,
    ff.s3_bucket_path,
    ff.file_name_pattern,
    ff.target_schema,
    ff.target_table,
    ff.delimiter,
    ff.has_header,
    ff.null_if,
    ff.compression,
    ff.load_frequency,
    ff.incremental_key,
    COUNT(DISTINCT df.data_field_id) AS total_fields,
    COUNT(DISTINCT dcm.mapping_id) AS total_mappings,
    MAX(fh.load_end_time) AS last_load_time,
    SUM(CASE WHEN fh.load_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_loads_count
FROM SWIGGY.COMMON.data_file_format ff
LEFT JOIN SWIGGY.COMMON.data_field df
    ON ff.file_format_id = df.file_format_id
LEFT JOIN SWIGGY.COMMON.database_column_mapping dcm
    ON df.data_field_id = dcm.data_field_id
LEFT JOIN SWIGGY.COMMON.file_load_history fh
    ON ff.file_format_id = fh.file_format_id
WHERE ff.is_active = TRUE
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13;