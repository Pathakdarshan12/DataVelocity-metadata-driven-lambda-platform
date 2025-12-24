-- =====================================================
-- SAMPLE METADATA CONFIGURATION
-- Food Delivery Transaction Data Example
-- =====================================================

-- ===== STEP 1: Define File Formats =====

-- Orders CSV file format
INSERT INTO swiggy.common.data_file_format (
    file_format_name, file_format_type, description, source_system,
    s3_bucket_path, file_name_pattern, delimiter, has_header, skip_header,
    field_optionally_enclosed_by, null_if, compression,
    target_schema, target_table, is_active, load_frequency, incremental_key
)
SELECT 'orders_csv', 'CSV', 'Daily order transaction files from food delivery system', 'delivery_app', 's3://food-delivery-data/raw/orders/', 'orders_*.csv', ',', TRUE, 1, '"', ARRAY_CONSTRUCT('NULL', 'null', '\\N', ''), 'GZIP', 'bronze', 'raw_orders', TRUE, 'DAILY', 'order_timestamp';

-- Customer JSON file format
INSERT INTO swiggy.common.data_file_format (
    file_format_name, file_format_type, description, source_system,
    s3_bucket_path, file_name_pattern, has_header,
    null_if, encoding,
    target_schema, target_table, is_active, load_frequency, incremental_key
) SELECT 'customers_json', 'JSON', 'Customer profile data in JSON format', 'crm_system', 's3://food-delivery-data/raw/customers/', 'customers_*.json', FALSE, ARRAY_CONSTRUCT('null', 'NULL'), 'UTF-8', 'bronze', 'raw_customers', TRUE, 'HOURLY', 'updated_at';

-- Restaurant CSV file format
INSERT INTO swiggy.common.data_file_format (
    file_format_name, file_format_type, description, source_system,
    s3_bucket_path, file_name_pattern, delimiter, has_header,
    field_optionally_enclosed_by, null_if,
    target_schema, target_table, is_active, load_frequency
) SELECT  'restaurants_csv', 'CSV', 'Restaurant master data', 'restaurant_portal', 's3://food-delivery-data/raw/restaurants/', 'restaurants_*.csv', ',', TRUE, '"', ARRAY_CONSTRUCT('NULL', ''), 'bronze', 'raw_restaurants', TRUE, 'DAILY';

-- -----------------------------------------------------
-- Define Data Fields for Orders CSV
-- -----------------------------------------------------

-- Order ID field
INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_position, field_data_type,
    field_length, is_nullable, description, validation_rules
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'orders_csv'),
    'order_id', 1, 'STRING', 50, FALSE, 'Unique order identifier', PARSE_JSON('{"pattern": "^ORD[0-9]{10}$", "min_length": 13, "max_length": 13}');

-- Customer ID field
INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_position, field_data_type,
    field_length, is_nullable, description
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'orders_csv'),
    'customer_id', 2, 'STRING', 50, FALSE, 'Customer identifier';

-- Restaurant ID field
INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_position, field_data_type,
    field_length, is_nullable, description
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'orders_csv'),
    'restaurant_id', 3, 'STRING', 50, FALSE, 'Restaurant identifier';

-- Order Timestamp field
INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_position, field_data_type,
    is_nullable, description, validation_rules
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'orders_csv'),
    'order_timestamp', 4, 'TIMESTAMP', FALSE, 'Order creation timestamp', PARSE_JSON('{"date_format": "YYYY-MM-DD HH24:MI:SS", "min_date": "2020-01-01"}');

-- Order Amount field
INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_position, field_data_type,
    field_precision, field_scale, is_nullable, description, validation_rules
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'orders_csv'),
    'order_amount', 5, 'DECIMAL', 10, 2, FALSE, 'Total order amount', PARSE_JSON('{"min_value": 0, "max_value": 10000}');

-- Order Status field
INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_position, field_data_type,
    field_length, is_nullable, description, validation_rules
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'orders_csv'),
    'order_status', 6, 'STRING', 50, FALSE, 'Current order status', PARSE_JSON('{"allowed_values": ["PLACED", "CONFIRMED", "PREPARING", "READY", "DISPATCHED", "DELIVERED", "CANCELLED"]}');

-- Delivery Fee field
INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_position, field_data_type,
    field_precision, field_scale, is_nullable, default_value, description
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'orders_csv'),
    'delivery_fee', 7, 'DECIMAL', 8, 2, TRUE, '0.00', 'Delivery charge';

-- ===== STEP 3: Define Data Fields for Customers JSON =====

INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_data_type, is_nullable,
    json_path, is_nested, description
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'customers_json'),
    'customer_id', 'STRING', FALSE, '$.customer_id', FALSE, 'Customer unique identifier';

INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_data_type, is_nullable,
    json_path, is_nested, description
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'customers_json'),
    'first_name', 'STRING', FALSE,'$.personal_info.first_name', TRUE, 'Customer first name';

INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_data_type, is_nullable,
    json_path, is_nested, description
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'customers_json'), 'last_name', 'STRING',
    FALSE, '$.personal_info.last_name', TRUE, 'Customer last name';

INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_data_type, is_nullable,
    json_path, is_nested, description
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'customers_json'),
    'email', 'STRING', FALSE, '$.contact.email', TRUE, 'Customer email address' ;

INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_data_type, is_nullable,
    json_path, is_nested, description
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'customers_json'),
    'phone', 'STRING', TRUE, '$.contact.phone', TRUE, 'Customer phone number';

INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_data_type, is_nullable,
    json_path, is_nested, description
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'customers_json'),
    'city', 'STRING', TRUE, '$.address.city', TRUE, 'Customer city';

INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_data_type, is_nullable,
    json_path, is_nested, description
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'customers_json'),
    'created_at', 'TIMESTAMP', FALSE, '$.created_at', FALSE, 'Account creation timestamp';

INSERT INTO swiggy.common.data_field (
    file_format_id, field_name, field_data_type, is_nullable,
    json_path, is_nested, description
) SELECT
    (SELECT file_format_id FROM swiggy.common.data_file_format WHERE file_format_name = 'customers_json'),
    'updated_at', 'TIMESTAMP', FALSE, '$.updated_at', FALSE, 'Last update timestamp' ;

-- ===== STEP 4: Define Database Column Mappings for Orders =====

-- Order ID mapping
INSERT INTO swiggy.common.database_column_mapping (
    data_field_id, target_schema, target_table, target_column,
    target_data_type, transformation_logic, transformation_type,
    is_primary_key, is_business_key, is_required, mapping_priority
) SELECT
    (SELECT df.data_field_id FROM swiggy.common.data_field df
     JOIN swiggy.common.data_file_format ff ON df.file_format_id = ff.file_format_id
     WHERE ff.file_format_name = 'orders_csv' AND df.field_name = 'order_id'),
    'bronze', 'raw_orders', 'order_id', 'VARCHAR(50)', 'UPPER(TRIM($1))', 'DIRECT', TRUE, TRUE, TRUE, 1;

-- Customer ID mapping
INSERT INTO swiggy.common.database_column_mapping (
    data_field_id, target_schema, target_table, target_column,
    target_data_type, transformation_logic, transformation_type,
    is_business_key, is_required, mapping_priority
) SELECT
    (SELECT df.data_field_id FROM swiggy.common.data_field df
     JOIN swiggy.common.data_file_format ff ON df.file_format_id = ff.file_format_id
     WHERE ff.file_format_name = 'orders_csv' AND df.field_name = 'customer_id'),
    'bronze', 'raw_orders', 'customer_id', 'VARCHAR(50)', 'UPPER(TRIM($2))', 'DIRECT', FALSE, TRUE, 2;

-- Restaurant ID mapping
INSERT INTO swiggy.common.database_column_mapping (
    data_field_id, target_schema, target_table, target_column,
    target_data_type, transformation_logic, transformation_type,
    is_business_key, is_required, mapping_priority
) SELECT
    (SELECT df.data_field_id FROM swiggy.common.data_field df
     JOIN swiggy.common.data_file_format ff ON df.file_format_id = ff.file_format_id
     WHERE ff.file_format_name = 'orders_csv' AND df.field_name = 'restaurant_id'),
    'bronze', 'raw_orders', 'restaurant_id', 'VARCHAR(50)', 'UPPER(TRIM($3))', 'DIRECT', FALSE, TRUE, 3;

-- Order Timestamp mapping
INSERT INTO swiggy.common.database_column_mapping (
    data_field_id, target_schema, target_table, target_column,
    target_data_type, transformation_logic, transformation_type,
    is_required, mapping_priority
) SELECT
    (SELECT df.data_field_id FROM swiggy.common.data_field df
     JOIN swiggy.common.data_file_format ff ON df.file_format_id = ff.file_format_id
     WHERE ff.file_format_name = 'orders_csv' AND df.field_name = 'order_timestamp'),
    'bronze', 'raw_orders', 'order_timestamp', 'TIMESTAMP_NTZ', 'TO_TIMESTAMP($4, ''YYYY-MM-DD HH24:MI:SS'')', 'DIRECT', TRUE, 4;

-- Order Amount mapping
INSERT INTO swiggy.common.database_column_mapping (
    data_field_id, target_schema, target_table, target_column,
    target_data_type, transformation_logic, transformation_type,
    is_required, mapping_priority, data_quality_rules
) SELECT
    (SELECT df.data_field_id FROM swiggy.common.data_field df
     JOIN swiggy.common.data_file_format ff ON df.file_format_id = ff.file_format_id
     WHERE ff.file_format_name = 'orders_csv' AND df.field_name = 'order_amount'),
    'bronze', 'raw_orders', 'order_amount', 'DECIMAL(10,2)', 'TRY_TO_DECIMAL($5, 10, 2)', 'DIRECT', TRUE, 5, PARSE_JSON('{"check": "order_amount >= 0 AND order_amount <= 10000"}');

-- Order Status mapping
INSERT INTO swiggy.common.database_column_mapping (
    data_field_id, target_schema, target_table, target_column,
    target_data_type, transformation_logic, transformation_type,
    is_required, mapping_priority
) SELECT
    (SELECT df.data_field_id FROM swiggy.common.data_field df
     JOIN swiggy.common.data_file_format ff ON df.file_format_id = ff.file_format_id
     WHERE ff.file_format_name = 'orders_csv' AND df.field_name = 'order_status'),
    'bronze', 'raw_orders', 'order_status', 'VARCHAR(50)', 'UPPER(TRIM($6))', 'DIRECT', TRUE, 6;

-- ===== STEP 5: Define Database Column Mappings for Customers =====

INSERT INTO swiggy.common.database_column_mapping (
    data_field_id, target_schema, target_table, target_column,
    target_data_type, transformation_logic, transformation_type,
    is_primary_key, is_business_key, is_scd_tracked, mapping_priority
) SELECT
    (SELECT df.data_field_id FROM swiggy.common.data_field df
     JOIN swiggy.common.data_file_format ff ON df.file_format_id = ff.file_format_id
     WHERE ff.file_format_name = 'customers_json' AND df.field_name = 'customer_id'),
     'bronze', 'raw_customers', 'customer_id', 'VARCHAR(50)', '$1:customer_id::VARCHAR', 'DIRECT', TRUE, TRUE, FALSE, 1;

INSERT INTO swiggy.common.database_column_mapping (
    data_field_id, target_schema, target_table, target_column,
    target_data_type, transformation_logic, transformation_type,
    is_primary_key, is_business_key, is_scd_tracked, mapping_priority
) SELECT
    (SELECT df.data_field_id FROM swiggy.common.data_field df
     JOIN swiggy.common.data_file_format ff ON df.file_format_id = ff.file_format_id
     WHERE ff.file_format_name = 'customers_json' AND df.field_name = 'first_name'),
    'bronze', 'raw_customers', 'first_name', 'VARCHAR(100)', '$1:personal_info.first_name::VARCHAR', 'DIRECT', FALSE, FALSE, TRUE, 2;

INSERT INTO swiggy.common.database_column_mapping (
    data_field_id, target_schema, target_table, target_column,
    target_data_type, transformation_logic, transformation_type,
    is_primary_key, is_business_key, is_scd_tracked, mapping_priority
) SELECT
    (SELECT df.data_field_id FROM swiggy.common.data_field df
     JOIN swiggy.common.data_file_format ff ON df.file_format_id = ff.file_format_id
     WHERE ff.file_format_name = 'customers_json' AND df.field_name = 'email'),
    'bronze', 'raw_customers', 'email', 'VARCHAR(200)', 'LOWER(TRIM($1:contact.email::VARCHAR))', 'DIRECT', FALSE, FALSE, TRUE, 4;

INSERT INTO swiggy.common.database_column_mapping (
    data_field_id, target_schema, target_table, target_column,
    target_data_type, transformation_logic, transformation_type,
    is_primary_key, is_business_key, is_scd_tracked, mapping_priority
) SELECT
    (SELECT df.data_field_id FROM swiggy.common.data_field df
     JOIN swiggy.common.data_file_format ff ON df.file_format_id = ff.file_format_id
     WHERE ff.file_format_name = 'customers_json' AND df.field_name = 'city'),
    'bronze', 'raw_customers', 'city', 'VARCHAR(100)', '$1:address.city::VARCHAR', 'DIRECT', FALSE, FALSE, TRUE, 6;


-- ===== STEP 6: Populate Junction Table (Optional - for many-to-many) =====

-- Link all order fields to the orders format
INSERT INTO swiggy.common.data_file_data_format (file_format_id, data_field_id, field_order, is_required_in_file, is_included_in_load)
SELECT
    ff.file_format_id,
    df.data_field_id,
    df.field_position,
    NOT df.is_nullable,
    TRUE
FROM swiggy.common.data_file_format ff
JOIN swiggy.common.data_field df ON ff.file_format_id = df.file_format_id
WHERE ff.file_format_name = 'orders_csv';

-- ===== STEP 7: Add Data Quality Rules =====

INSERT INTO swiggy.common.data_quality_rules (
    rule_name, rule_type, rule_category, rule_expression, rule_description, severity
) VALUES
('positive_amount', 'RANGE', 'VALIDITY', 'order_amount > 0', 'Order amount must be positive', 'ERROR'),
('valid_email_format', 'PATTERN', 'VALIDITY', 'email RLIKE ''^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$''', 'Email must be valid format', 'ERROR'),
('future_date_check', 'CUSTOM', 'ACCURACY', 'order_timestamp <= CURRENT_TIMESTAMP()', 'Order timestamp cannot be in future', 'WARNING'),
('order_id_format', 'PATTERN', 'VALIDITY', 'order_id RLIKE ''^ORD[0-9]{10}$''', 'Order ID must match pattern ORD + 10 digits', 'ERROR');

-- ===== VERIFICATION QUERIES =====

-- Show complete mapping for orders
SELECT * FROM swiggy.common.vw_complete_field_mapping
WHERE file_format_name = 'orders_csv'
ORDER BY source_field_name;

-- Show file processing configuration
SELECT * FROM swiggy.common.vw_file_processing_config;