-- =============================================================================
-- sql/ddl/01_create_tables.sql
--
-- Creates all eight metadata tables used by the data-validation framework.
-- Tables are created in dependency order (parents before children).
--
-- Run after 00_create_database.sql:
--   mysql -h <host> -u root -p Data_Validation < sql/ddl/01_create_tables.sql
-- =============================================================================

USE `Data_Validation`;

-- ---------------------------------------------------------------------------
-- 1. data_validation_rule
--    Stores individual validation rules (both Custodial and Business).
--    Rule_Logic holds a JSON constraint dict (custodial) or a SQL string
--    (business) that the framework evaluates against source data.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `data_validation_rule` (
    `Rule_ID`          INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    `Rule_Description` VARCHAR(500)    NOT NULL COMMENT 'Human-readable description of the rule',
    `Rule_Logic`       MEDIUMTEXT      NOT NULL COMMENT 'JSON constraint dict (custodial) or full SQL statement (business)',
    `Test_Type`        ENUM('Business', 'Custodial')
                                       NOT NULL COMMENT 'Business = SQL push-down; Custodial = schema/quality test',
    `Created_At`       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `Updated_At`       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`Rule_ID`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Validation rule definitions';


-- ---------------------------------------------------------------------------
-- 2. data_validation_object_lookup
--    Registry of all source objects (BigQuery tables, Cloud SQL tables, GCS
--    files) that the framework validates.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `data_validation_object_lookup` (
    `Object_ID`            INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    `Object_Name`          VARCHAR(255)    NOT NULL COMMENT 'Table name or GCS filename (without extension)',
    `Object_Database_Name` VARCHAR(255)    NOT NULL COMMENT 'BigQuery dataset, MySQL schema, or GCS bucket path',
    `Object_Extension`     VARCHAR(10)     NULL     COMMENT 'GCS file extension: csv | json | xml (NULL for DB sources)',
    `Primary_Key`          VARCHAR(500)    NOT NULL COMMENT 'Comma-separated primary key column names',
    `Validation_Period`    ENUM('full table', 'daily', 'monthly', 'yearly')
                                           NOT NULL DEFAULT 'full table'
                                           COMMENT 'Granularity of the validation window',
    `Time_Interval`        INT UNSIGNED    NULL     COMMENT 'Custom interval in minutes (overrides Validation_Period when set)',
    `Validation_Frequency` VARCHAR(100)    NULL     COMMENT 'Human-readable frequency label (e.g. "Daily at midnight")',
    `Active`               TINYINT(1)      NOT NULL DEFAULT 1 COMMENT '1 = active, 0 = disabled',
    `Created_At`           TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `Updated_At`           TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`Object_ID`),
    INDEX `idx_object_name_db` (`Object_Name`, `Object_Database_Name`),
    INDEX `idx_active` (`Active`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Registry of source objects (tables / files) subject to validation';


-- ---------------------------------------------------------------------------
-- 3. data_validation_rule_mapping
--    Associates rules with objects and specifies which columns and
--    participating tables are involved in each validation.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `data_validation_rule_mapping` (
    `Mapping_ID`             INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    `Object_ID`              INT UNSIGNED    NOT NULL,
    `Rule_ID`                INT UNSIGNED    NOT NULL,
    `Column_Name`            VARCHAR(500)    NOT NULL COMMENT 'Comma-separated target column name(s)',
    `participating_table`    VARCHAR(1000)   NULL     COMMENT 'Comma-separated fully-qualified BigQuery table(s) to load as Spark views',
    `Source_Table_Date_Col`  VARCHAR(255)    NULL     COMMENT 'Date column used for delta/incremental filtering',
    `Test_Type`              ENUM('Business', 'Custodial')
                                             NOT NULL COMMENT 'Mirrors data_validation_rule.Test_Type for fast filtering',
    `Active`                 TINYINT(1)      NOT NULL DEFAULT 1,
    `Created_At`             TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `Updated_At`             TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`Mapping_ID`),
    CONSTRAINT `fk_mapping_object`
        FOREIGN KEY (`Object_ID`) REFERENCES `data_validation_object_lookup` (`Object_ID`)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT `fk_mapping_rule`
        FOREIGN KEY (`Rule_ID`) REFERENCES `data_validation_rule` (`Rule_ID`)
        ON DELETE CASCADE ON UPDATE CASCADE,
    INDEX `idx_mapping_object` (`Object_ID`),
    INDEX `idx_mapping_rule`   (`Rule_ID`),
    INDEX `idx_mapping_active` (`Active`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Links validation rules to source objects with column-level detail';


-- ---------------------------------------------------------------------------
-- 4. data_validation_rule_threshold
--    Defines criticality levels for each rule mapping.  When the failure
--    percentage exceeds Failure_Threshold_Value, Criticality is assigned.
--    Multiple threshold rows per Mapping_ID allow tiered severity levels.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `data_validation_rule_threshold` (
    `Threshold_ID`           INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    `Mapping_ID`             INT UNSIGNED    NOT NULL,
    `Failure_Threshold_Value` DECIMAL(5, 4)  NOT NULL COMMENT 'Fractional failure rate trigger (e.g. 0.05 = 5%)',
    `Criticality`            TINYINT UNSIGNED NOT NULL COMMENT 'Criticality level: 1 = low, 2 = medium, 3 = high',
    `Active`                 TINYINT(1)      NOT NULL DEFAULT 1,
    `Created_At`             TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `Updated_At`             TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`Threshold_ID`),
    CONSTRAINT `fk_threshold_mapping`
        FOREIGN KEY (`Mapping_ID`) REFERENCES `data_validation_rule_mapping` (`Mapping_ID`)
        ON DELETE CASCADE ON UPDATE CASCADE,
    INDEX `idx_threshold_mapping` (`Mapping_ID`),
    INDEX `idx_threshold_active`  (`Active`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Failure-rate thresholds that determine alert criticality levels';


-- ---------------------------------------------------------------------------
-- 5. data_validation_notification_message_handler
--    Stores named notification message templates that describe what the alert
--    means to the recipient.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `data_validation_notification_message_handler` (
    `Notification_Handler_ID`   INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    `Notification_Handler_Text` VARCHAR(1000)   NOT NULL COMMENT 'Message body shown in the alert email',
    `Created_At`                TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `Updated_At`                TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`Notification_Handler_ID`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Alert message templates';


-- ---------------------------------------------------------------------------
-- 6. data_validation_notification_distribution_list
--    Holds recipient email addresses for notification groups.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `data_validation_notification_distribution_list` (
    `Notification_Distribution_ID`      INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    `Notification_Distribution_Contact` VARCHAR(320)    NOT NULL COMMENT 'Recipient email address (RFC 5321 max = 320 chars)',
    `Created_At`                        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `Updated_At`                        TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`Notification_Distribution_ID`),
    INDEX `idx_dist_contact` (`Notification_Distribution_Contact`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Email distribution list for validation failure alerts';


-- ---------------------------------------------------------------------------
-- 7. data_validation_notification
--    Joins a message handler with a distribution list to form a complete
--    notification configuration.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `data_validation_notification` (
    `Notification_ID`              INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    `Notification_Distribution_ID` INT UNSIGNED    NOT NULL,
    `Notification_Handler_ID`      INT UNSIGNED    NOT NULL,
    `Created_At`                   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `Updated_At`                   TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`Notification_ID`),
    CONSTRAINT `fk_notification_dist`
        FOREIGN KEY (`Notification_Distribution_ID`)
        REFERENCES `data_validation_notification_distribution_list` (`Notification_Distribution_ID`)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT `fk_notification_handler`
        FOREIGN KEY (`Notification_Handler_ID`)
        REFERENCES `data_validation_notification_message_handler` (`Notification_Handler_ID`)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Pairs a distribution list with a message handler to form a notification config';


-- ---------------------------------------------------------------------------
-- 8. data_validation_object_notification
--    Links a source object to a notification configuration and specifies
--    the minimum criticality level required to trigger the alert.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `data_validation_object_notification` (
    `Object_Notification_ID` INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    `ObjectID`               INT UNSIGNED    NOT NULL COMMENT 'FK → data_validation_object_lookup.Object_ID',
    `Notification_ID`        INT UNSIGNED    NOT NULL,
    `Criticality`            TINYINT UNSIGNED NOT NULL COMMENT 'Minimum criticality level to trigger this notification',
    `Created_At`             TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `Updated_At`             TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`Object_Notification_ID`),
    CONSTRAINT `fk_obj_notif_object`
        FOREIGN KEY (`ObjectID`) REFERENCES `data_validation_object_lookup` (`Object_ID`)
        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT `fk_obj_notif_notification`
        FOREIGN KEY (`Notification_ID`) REFERENCES `data_validation_notification` (`Notification_ID`)
        ON DELETE CASCADE ON UPDATE CASCADE,
    INDEX `idx_obj_notif_object`   (`ObjectID`),
    INDEX `idx_obj_notif_criticality` (`Criticality`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Associates source objects with notification configs and criticality gates';
