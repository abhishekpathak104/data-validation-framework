-- =============================================================================
-- sql/dml/01_seed_rules.sql
--
-- Inserts sample validation rules into data_validation_rule.
-- Covers common custodial (schema/quality) tests and one business rule.
--
-- Custodial Rule_Logic format: JSON dict of { test_key: test_value }
-- Business Rule_Logic format:  Full SQL SELECT statement returning failing rows
-- =============================================================================

USE `Data_Validation`;

INSERT INTO `data_validation_rule`
    (`Rule_ID`, `Rule_Description`, `Rule_Logic`, `Test_Type`)
VALUES

-- ── Custodial rules ──────────────────────────────────────────────────────────

(1,
 'Primary key must not be null',
 '{"is_nullable": "NO"}',
 'Custodial'),

(2,
 'Primary key values must be unique (no duplicates)',
 '{"distinct": true}',
 'Custodial'),

(3,
 'Email address must match RFC 5321 format',
 '{"regex": "^[a-zA-Z0-9._%+\\\\-]+@[a-zA-Z0-9.\\\\-]+\\\\.[a-zA-Z]{2,}$"}',
 'Custodial'),

(4,
 'Phone number must be 10 digits',
 '{"regex": "^[0-9]{10}$"}',
 'Custodial'),

(5,
 'Status must be one of the allowed values',
 '{"allowed": ["Active", "Inactive", "Pending", "Closed"]}',
 'Custodial'),

(6,
 'Test type must not be a forbidden value',
 '{"forbidden": ["UNKNOWN", "NULL", "N/A"]}',
 'Custodial'),

(7,
 'Amount must be greater than or equal to 0 (min value check)',
 '{"min": 0}',
 'Custodial'),

(8,
 'Discount percentage must not exceed 100 (max value check)',
 '{"max": 100}',
 'Custodial'),

(9,
 'Name field must be at least 2 characters',
 '{"min_length": 2}',
 'Custodial'),

(10,
 'Description field must not exceed 500 characters',
 '{"max_length": 500}',
 'Custodial'),

(11,
 'Transaction date must be on or after 2020-01-01',
 '{"min_date": "2020-01-01"}',
 'Custodial'),

(12,
 'Transaction date must not be in the future',
 '{"max_date": "CURRENT_DATE"}',
 'Custodial'),

(13,
 'All column data types must match the BigQuery reference schema',
 '{}',
 'Custodial'),

(14,
 'Custom SQL: flag records where amount is negative but status is Active',
 '{"custom_sql": "SELECT * FROM dfSQL WHERE amount < 0 AND status = ''Active''"}',
 'Custodial'),

-- ── Business rules ───────────────────────────────────────────────────────────

(15,
 'Business rule: orders with no corresponding customer record',
 'SELECT o.order_id, o.customer_id, o.order_date, o.amount
  FROM `project.dataset.orders` o
  LEFT JOIN `project.dataset.customers` c ON o.customer_id = c.customer_id
  WHERE c.customer_id IS NULL
    AND o.order_date >= ''{val_st_dt}''
    AND o.order_date <  ''{val_end_dt}'';',
 'Business'),

(16,
 'Business rule: invoices where amount does not match sum of line items',
 'SELECT i.invoice_id, i.total_amount, COALESCE(SUM(l.line_amount), 0) AS line_sum
  FROM `project.dataset.invoices` i
  LEFT JOIN `project.dataset.invoice_lines` l ON i.invoice_id = l.invoice_id
  WHERE i.invoice_date >= ''{val_st_dt}''
    AND i.invoice_date <  ''{val_end_dt}''
  GROUP BY i.invoice_id, i.total_amount
  HAVING ABS(i.total_amount - line_sum) > 0.01;',
 'Business');
