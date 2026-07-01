-- =============================================================================
-- sql/dml/03_seed_mappings.sql
--
-- Maps validation rules to source objects in data_validation_rule_mapping.
-- Each row wires one rule to one object, specifying the target column(s),
-- any participating lookup tables, and the date column for delta filtering.
--
-- Depends on: 01_seed_rules.sql, 02_seed_objects.sql
-- =============================================================================

USE `Data_Validation`;

INSERT INTO `data_validation_rule_mapping`
    (`Mapping_ID`, `Object_ID`, `Rule_ID`, `Column_Name`,
     `participating_table`, `Source_Table_Date_Col`, `Test_Type`, `Active`)
VALUES

-- ── customers (Object_ID = 1) ────────────────────────────────────────────────

-- customer_id must not be null
(1,  1, 1, 'customer_id',   NULL,                         NULL,           'Custodial', 1),
-- customer_id must be unique
(2,  1, 2, 'customer_id',   NULL,                         NULL,           'Custodial', 1),
-- email must match regex
(3,  1, 3, 'email',         NULL,                         NULL,           'Custodial', 1),
-- status must be in allowed values
(4,  1, 5, 'status',        NULL,                         NULL,           'Custodial', 1),
-- name must be at least 2 chars
(5,  1, 9, 'name',          NULL,                         NULL,           'Custodial', 1),
-- data types must match BQ schema
(6,  1, 13,'customer_id',   NULL,                         NULL,           'Custodial', 1),

-- ── orders (Object_ID = 2) ───────────────────────────────────────────────────

-- order_id must not be null
(7,  2, 1, 'order_id',      NULL,                         'order_date',   'Custodial', 1),
-- order_id must be unique
(8,  2, 2, 'order_id',      NULL,                         'order_date',   'Custodial', 1),
-- amount >= 0
(9,  2, 7, 'amount',        NULL,                         'order_date',   'Custodial', 1),
-- order_date must be >= 2020-01-01
(10, 2, 11,'order_date',    NULL,                         'order_date',   'Custodial', 1),
-- order_date must not be in the future
(11, 2, 12,'order_date',    NULL,                         'order_date',   'Custodial', 1),
-- business rule: orders with no matching customer
(12, 2, 15,'order_id, customer_id',
                             'salesforce.orders,salesforce.customers',
                                                           'order_date',   'Business',  1),

-- ── invoices (Object_ID = 3) ─────────────────────────────────────────────────

-- invoice_id not null
(13, 3, 1, 'invoice_id',    NULL,                         'invoice_date', 'Custodial', 1),
-- invoice_id unique
(14, 3, 2, 'invoice_id',    NULL,                         'invoice_date', 'Custodial', 1),
-- amount >= 0
(15, 3, 7, 'total_amount',  NULL,                         'invoice_date', 'Custodial', 1),
-- business rule: invoice total ≠ sum of lines
(16, 3, 16,'invoice_id, total_amount',
                             'salesforce.invoices,salesforce.invoice_lines',
                                                           'invoice_date', 'Business',  1),

-- ── products (Object_ID = 4, Cloud SQL) ──────────────────────────────────────

-- product_id not null
(17, 4, 1, 'product_id',    NULL,                         NULL,           'Custodial', 1),
-- product_id unique
(18, 4, 2, 'product_id',    NULL,                         NULL,           'Custodial', 1),
-- price >= 0
(19, 4, 7, 'price',         NULL,                         NULL,           'Custodial', 1),
-- description max 500 chars
(20, 4, 10,'description',   NULL,                         NULL,           'Custodial', 1),

-- ── account GCS CSV (Object_ID = 5) ──────────────────────────────────────────

-- Id not null
(21, 5, 1, 'Id',            NULL,                         NULL,           'Custodial', 1),
-- Id unique
(22, 5, 2, 'Id',            NULL,                         NULL,           'Custodial', 1),
-- data types match schema
(23, 5, 13,'Id',            NULL,                         NULL,           'Custodial', 1),

-- ── events (Object_ID = 7) ───────────────────────────────────────────────────

-- event_id not null
(24, 7, 1, 'event_id',      NULL,                         'event_time',   'Custodial', 1),
-- event_id unique
(25, 7, 2, 'event_id',      NULL,                         'event_time',   'Custodial', 1),
-- event_time >= 2020-01-01
(26, 7, 11,'event_time',    NULL,                         'event_time',   'Custodial', 1);
