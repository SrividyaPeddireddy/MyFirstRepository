-- Databricks notebook source
USE CATALOG `unity-catalog-pov`;

-- COMMAND ----------

USE SCHEMA `dbx-uc-pov-db`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Create Delta table**

-- COMMAND ----------

CREATE TABLE customers


-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Load data from a S3 location into a Delta table**

-- COMMAND ----------

COPY INTO customers
FROM 's3://databricksunitycatalogtest/CNX/customers.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

SELECT * FROM customers;

-- COMMAND ----------

SELECT current_user();

-- COMMAND ----------

SELECT is_member('admins')

-- COMMAND ----------

select is_account_group_member('admins')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Example of Column-level permissions at individual user level**

-- COMMAND ----------

CREATE OR REPLACE VIEW customers_redacted AS
SELECT
	CUSTOMERID,
    CUSTOMERNAME,
CASE WHEN
  current_user() = 'srividya.peddireddy@concentrix.com' THEN EMAIL
  ELSE '*****'
END AS EMAIL,
	CITY,
	COUNTRY,
    TERRITORY
FROM customers

-- COMMAND ----------

SELECT * FROM customers_redacted;

-- COMMAND ----------

GRANT SELECT ON customers_redacted TO `mahalakshmi.kannan@concentrix.com`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Example of Column-level permissions at group level**

-- COMMAND ----------

CREATE VIEW customers_redacted_admin AS
SELECT
    CUSTOMERID,
	CUSTOMERNAME,
CASE WHEN
  is_member('admins') THEN EMAIL
  ELSE '*****'
END AS EMAIL,
	CITY,
	COUNTRY,
    TERRITORY
FROM customers

-- COMMAND ----------

CREATE OR REPLACE VIEW customers_redacted_groups AS
SELECT
    CUSTOMERID,
	CUSTOMERNAME,
  CASE 
WHEN is_account_group_member('admins') THEN EMAIL
WHEN is_member('dbx-cnx') THEN EMAIL
WHEN is_member('admins') THEN EMAIL
  ELSE '*****'
END AS EMAIL,
	CITY,
	COUNTRY,
    TERRITORY
FROM customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Example of Row-level permissions at Individual user level**

-- COMMAND ----------

CREATE OR REPLACE VIEW customers_row_filter AS
SELECT
	CUSTOMERID,
	CUSTOMERNAME,
	COUNTRY,
	TERRITORY
FROM customers
WHERE
CASE WHEN
  current_user() = 'srividya.peddireddy@concentrix.com' THEN TRUE
  ELSE COUNTRY = 'USA'
END;

-- COMMAND ----------

SELECT * FROM customers_row_filter;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Example of Row-level permissions at group level**

-- COMMAND ----------

CREATE OR REPLACE VIEW customers_row_filter_admin AS
SELECT
	CUSTOMERID,
	CUSTOMERNAME,
	COUNTRY,
	TERRITORY
FROM customers
WHERE
CASE WHEN
  is_member('admins') THEN TRUE
  ELSE COUNTRY = 'USA'
END;

-- COMMAND ----------

select * from customers_row_filter_admin;

-- COMMAND ----------

CREATE EXTERNAL TABLE test_ext
LOCATION 's3://databricksunitycatalogtest/test-external/'
WITH (CREDENTIAL `cc4712d9-ad4a-4c36-a63a-a2160981a555-data-access-config-1663857552519`);

-- COMMAND ----------

COPY INTO test_ext
FROM 's3://databricksunitycatalogtest/CNX/customers.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

SELECT * FROM test_ext;

-- COMMAND ----------

create table claims

-- COMMAND ----------

COPY INTO claims
FROM 's3://databricksunitycatalogtest/datasets/sampledata/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

select count(DISTINCT claim_id) from claims

-- COMMAND ----------

select * from claims limit 10

-- COMMAND ----------

CREATE OR REPLACE VIEW claims_redacted AS
SELECT
	claim_id,
    service_date,
CASE WHEN
  current_user() = 'srividya.peddireddy@concentrix.com' THEN price
  ELSE '*****'
END AS price
FROM claims

-- COMMAND ----------

CREATE OR REPLACE VIEW claims_row_filter AS
SELECT
	claim_id,
	service_date,
    price
FROM claims
WHERE
CASE WHEN
  current_user() = 'srividya.peddireddy@@concentrix.com' THEN TRUE
  ELSE service_date = '2022/09/30'
END;

-- COMMAND ----------

select * from claims_row_filter;

-- COMMAND ----------

describe table extended claims;

-- COMMAND ----------

describe table extended test_claims;

-- COMMAND ----------

select * from test_claims limit 5;

-- COMMAND ----------

CREATE TABLE persons(first_name STRING NOT NULL, last_name STRING NOT NULL, nickname STRING,
                       CONSTRAINT persons_pk PRIMARY KEY(first_name, last_name));

-- COMMAND ----------

INSERT INTO persons (first_name, last_name, nickname)
VALUES ("Srividya", "Peddireddy", "Vidya");

-- COMMAND ----------

INSERT INTO persons (first_name, last_name, nickname)
VALUES ("Mahalakshmi", "Kannan", "Maha");

-- COMMAND ----------

INSERT INTO persons (first_name, last_name, nickname)
VALUES ("Sharumathi", "S", "Sharu");

-- COMMAND ----------

DESCRIBE TABLE EXTENDED persons;

-- COMMAND ----------

INSERT INTO persons (first_name, last_name, nickname)
VALUES ("Srividya", "Peddireddy", "Sri");

-- COMMAND ----------

SELECT * FROM persons;

-- COMMAND ----------

CREATE TABLE persons1(first_name STRING NOT NULL, last_name STRING NOT NULL, nickname STRING,
                       CONSTRAINT persons1_pk PRIMARY KEY(first_name, last_name) ENABLED);
