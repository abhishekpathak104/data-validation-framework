-- =============================================================================
-- sql/ddl/00_create_database.sql
--
-- Creates the Data_Validation metadata database and user.
-- Run this script once as a MySQL administrator before executing
-- 01_create_tables.sql.
--
-- Usage:
--   mysql -h <host> -u root -p < sql/ddl/00_create_database.sql
-- =============================================================================

CREATE DATABASE IF NOT EXISTS `Data_Validation`
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- Create an application user with least-privilege access.
-- Replace 'app_password' with a strong password or use a secrets manager.
-- CREATE USER IF NOT EXISTS 'dv_app'@'%' IDENTIFIED BY 'app_password';
-- GRANT SELECT, INSERT, UPDATE ON `Data_Validation`.* TO 'dv_app'@'%';
-- FLUSH PRIVILEGES;

USE `Data_Validation`;
