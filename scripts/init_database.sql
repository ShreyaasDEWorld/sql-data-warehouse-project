
-- Database: Datewarehouse

-- DROP DATABASE IF EXISTS "Datewarehouse";

CREATE DATABASE "Datewarehouse"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_India.1252'
    LC_CTYPE = 'English_India.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;


	-- SCHEMA: bronze

-- DROP SCHEMA IF EXISTS bronze ;

CREATE SCHEMA IF NOT EXISTS bronze
    AUTHORIZATION postgres;

	-- SCHEMA: silver

-- DROP SCHEMA IF EXISTS silver ;

CREATE SCHEMA IF NOT EXISTS silver
    AUTHORIZATION postgres;



	-- SCHEMA: gold

-- DROP SCHEMA IF EXISTS gold ;

CREATE SCHEMA IF NOT EXISTS gold
    AUTHORIZATION postgres;
