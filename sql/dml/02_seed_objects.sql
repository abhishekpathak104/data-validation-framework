-- =============================================================================
-- sql/dml/02_seed_objects.sql
--
-- Registers sample source objects in data_validation_object_lookup.
-- Each row represents one table, file, or dataset subject to validation.
--
-- Object_Extension: set to 'csv', 'json', or 'xml' for GCS file sources;
--                   leave NULL for BigQuery / Cloud SQL table sources.
-- Validation_Period: 'full table' = scan entire table each run;
--                    'daily' / 'monthly' / 'yearly' = incremental delta.
-- Time_Interval:     override in minutes (takes priority over Validation_Period).
-- =============================================================================

USE `Data_Validation`;

INSERT INTO `data_validation_object_lookup`
    (`Object_ID`, `Object_Name`, `Object_Database_Name`, `Object_Extension`,
     `Primary_Key`, `Validation_Period`, `Time_Interval`, `Validation_Frequency`, `Active`)
VALUES

-- ── BigQuery table sources ───────────────────────────────────────────────────
(1,
 'customers',
 'salesforce',          -- BigQuery dataset name
 NULL,
 'customer_id',
 'daily',
 NULL,
 'Every day at 06:00 UTC',
 1),

(2,
 'orders',
 'salesforce',
 NULL,
 'order_id',
 'daily',
 NULL,
 'Every day at 07:00 UTC',
 1),

(3,
 'invoices',
 'salesforce',
 NULL,
 'invoice_id',
 'monthly',
 NULL,
 'First day of each month at 08:00 UTC',
 1),

-- ── Cloud SQL (MySQL) table source ───────────────────────────────────────────
(4,
 'products',
 'ecommerce_db',        -- MySQL schema name
 NULL,
 'product_id',
 'full table',
 NULL,
 'Weekly on Sunday at 02:00 UTC',
 1),

-- ── GCS file sources ─────────────────────────────────────────────────────────
(5,
 'account',             -- matched against GCS filename: account.csv
 'salesforce',          -- BigQuery dataset used for schema reference
 'csv',
 'Id',
 'full table',
 NULL,
 'On file upload',
 1),

(6,
 'transaction',
 'finance',
 'json',
 'transaction_id',
 'full table',
 NULL,
 'On file upload',
 1),

-- ── Example with custom time interval ────────────────────────────────────────
(7,
 'events',
 'analytics',
 NULL,
 'event_id',
 'daily',
 60,                    -- override: validate in 60-minute windows
 'Hourly',
 1);
