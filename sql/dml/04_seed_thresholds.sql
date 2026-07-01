-- =============================================================================
-- sql/dml/04_seed_thresholds.sql
--
-- Defines criticality thresholds per rule mapping in
-- data_validation_rule_threshold.
--
-- Failure_Threshold_Value is a fractional failure rate (0.0 – 1.0):
--   0.01  =  1% of records failing
--   0.05  =  5% of records failing
--   0.10  = 10% of records failing
--
-- Criticality levels (convention used in this framework):
--   1 = Low      → informational alert only
--   2 = Medium   → alert team lead
--   3 = High     → alert team lead + escalation group
--
-- Multiple rows per Mapping_ID create tiered escalation:
--   Criticality 1 fires at  1%, Criticality 2 at 5%, Criticality 3 at 10%.
--
-- Depends on: 03_seed_mappings.sql
-- =============================================================================

USE `Data_Validation`;

INSERT INTO `data_validation_rule_threshold`
    (`Mapping_ID`, `Failure_Threshold_Value`, `Criticality`, `Active`)
VALUES

-- ── customers: customer_id not-null (Mapping_ID = 1) ─────────────────────────
--   Any null primary key → immediate high criticality
(1,  0.0000, 3, 1),

-- ── customers: customer_id unique (Mapping_ID = 2) ───────────────────────────
(2,  0.0000, 3, 1),

-- ── customers: email regex (Mapping_ID = 3) ──────────────────────────────────
(3,  0.0100, 1, 1),   -- 1% → Low
(3,  0.0500, 2, 1),   -- 5% → Medium
(3,  0.1000, 3, 1),   -- 10% → High

-- ── customers: status allowed values (Mapping_ID = 4) ────────────────────────
(4,  0.0100, 1, 1),
(4,  0.0500, 2, 1),
(4,  0.1000, 3, 1),

-- ── customers: name min_length (Mapping_ID = 5) ──────────────────────────────
(5,  0.0100, 1, 1),
(5,  0.0500, 2, 1),

-- ── customers: data types (Mapping_ID = 6) ───────────────────────────────────
(6,  0.0000, 2, 1),   -- any type mismatch → Medium alert

-- ── orders: order_id not-null (Mapping_ID = 7) ───────────────────────────────
(7,  0.0000, 3, 1),

-- ── orders: order_id unique (Mapping_ID = 8) ─────────────────────────────────
(8,  0.0000, 3, 1),

-- ── orders: amount >= 0 (Mapping_ID = 9) ─────────────────────────────────────
(9,  0.0050, 1, 1),   -- 0.5% → Low
(9,  0.0100, 2, 1),   -- 1%   → Medium
(9,  0.0500, 3, 1),   -- 5%   → High

-- ── orders: order_date min_date (Mapping_ID = 10) ────────────────────────────
(10, 0.0000, 2, 1),

-- ── orders: order_date max_date (Mapping_ID = 11) ────────────────────────────
(11, 0.0000, 3, 1),   -- future date is always high severity

-- ── orders: business rule – orphan orders (Mapping_ID = 12) ──────────────────
(12, 0.0010, 1, 1),   -- 0.1% → Low
(12, 0.0050, 2, 1),   -- 0.5% → Medium
(12, 0.0100, 3, 1),   -- 1%   → High

-- ── invoices: invoice_id not-null (Mapping_ID = 13) ──────────────────────────
(13, 0.0000, 3, 1),

-- ── invoices: invoice_id unique (Mapping_ID = 14) ────────────────────────────
(14, 0.0000, 3, 1),

-- ── invoices: total_amount >= 0 (Mapping_ID = 15) ────────────────────────────
(15, 0.0000, 2, 1),

-- ── invoices: business rule – total ≠ line sum (Mapping_ID = 16) ─────────────
(16, 0.0010, 2, 1),
(16, 0.0100, 3, 1),

-- ── products: product_id not-null (Mapping_ID = 17) ──────────────────────────
(17, 0.0000, 3, 1),

-- ── products: product_id unique (Mapping_ID = 18) ────────────────────────────
(18, 0.0000, 3, 1),

-- ── products: price >= 0 (Mapping_ID = 19) ───────────────────────────────────
(19, 0.0000, 2, 1),

-- ── products: description max_length (Mapping_ID = 20) ───────────────────────
(20, 0.0100, 1, 1),
(20, 0.0500, 2, 1),

-- ── account CSV: Id not-null (Mapping_ID = 21) ───────────────────────────────
(21, 0.0000, 3, 1),

-- ── account CSV: Id unique (Mapping_ID = 22) ─────────────────────────────────
(22, 0.0000, 3, 1),

-- ── account CSV: data types (Mapping_ID = 23) ────────────────────────────────
(23, 0.0000, 2, 1),

-- ── events: event_id not-null (Mapping_ID = 24) ──────────────────────────────
(24, 0.0000, 3, 1),

-- ── events: event_id unique (Mapping_ID = 25) ────────────────────────────────
(25, 0.0000, 3, 1),

-- ── events: event_time min_date (Mapping_ID = 26) ────────────────────────────
(26, 0.0000, 1, 1);
