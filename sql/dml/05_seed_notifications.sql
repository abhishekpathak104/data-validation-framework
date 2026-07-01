-- =============================================================================
-- sql/dml/05_seed_notifications.sql
--
-- Configures email notification groups and links them to source objects.
--
-- Insert order:
--   1. data_validation_notification_message_handler  (message templates)
--   2. data_validation_notification_distribution_list (recipient emails)
--   3. data_validation_notification                  (handler + dist join)
--   4. data_validation_object_notification           (object → notification)
--
-- Replace placeholder email addresses with real distribution lists.
--
-- Depends on: 02_seed_objects.sql
-- =============================================================================

USE `Data_Validation`;

-- ---------------------------------------------------------------------------
-- 1. Message handler templates
--    Notification_Handler_Text is included in the alert email body.
-- ---------------------------------------------------------------------------
INSERT INTO `data_validation_notification_message_handler`
    (`Notification_Handler_ID`, `Notification_Handler_Text`)
VALUES
(1, 'LOW SEVERITY: Data quality issues detected. Please review the attached validation error report at your earliest convenience.'),
(2, 'MEDIUM SEVERITY: Significant data quality issues detected. Please investigate and remediate within 24 hours.'),
(3, 'HIGH SEVERITY: Critical data quality failures detected. Immediate action is required. Please escalate to the data engineering team.');


-- ---------------------------------------------------------------------------
-- 2. Distribution lists
--    Add one row per unique recipient email address.
-- ---------------------------------------------------------------------------
INSERT INTO `data_validation_notification_distribution_list`
    (`Notification_Distribution_ID`, `Notification_Distribution_Contact`)
VALUES
(1, 'data-quality-team@example.com'),
(2, 'data-engineering-lead@example.com'),
(3, 'escalation-group@example.com');


-- ---------------------------------------------------------------------------
-- 3. Notification configs
--    Join each handler template with a distribution list.
--    Three tiers: Low → team, Medium → lead, High → escalation group.
-- ---------------------------------------------------------------------------
INSERT INTO `data_validation_notification`
    (`Notification_ID`, `Notification_Distribution_ID`, `Notification_Handler_ID`)
VALUES
--  Low severity  → data quality team + Low message
(1, 1, 1),
--  Medium severity → team lead + Medium message
(2, 2, 2),
--  High severity → escalation group + High message
(3, 3, 3);


-- ---------------------------------------------------------------------------
-- 4. Object–notification associations
--    Link each source object to the appropriate notification configs.
--    Criticality column controls the minimum level that triggers the alert.
--
--    Convention:
--      Criticality 1 (Low)    → Notification_ID 1  (team email)
--      Criticality 2 (Medium) → Notification_ID 2  (lead email)
--      Criticality 3 (High)   → Notification_ID 3  (escalation email)
-- ---------------------------------------------------------------------------
INSERT INTO `data_validation_object_notification`
    (`ObjectID`, `Notification_ID`, `Criticality`)
VALUES

-- customers (Object_ID = 1)
(1, 1, 1),   -- Low  → team
(1, 2, 2),   -- Medium → lead
(1, 3, 3),   -- High → escalation

-- orders (Object_ID = 2)
(2, 1, 1),
(2, 2, 2),
(2, 3, 3),

-- invoices (Object_ID = 3)
(3, 1, 1),
(3, 2, 2),
(3, 3, 3),

-- products (Object_ID = 4)
(4, 1, 1),
(4, 2, 2),

-- account GCS CSV (Object_ID = 5)
(5, 2, 2),   -- Medium and above only
(5, 3, 3),

-- events (Object_ID = 7)
(7, 1, 1),
(7, 2, 2),
(7, 3, 3);
