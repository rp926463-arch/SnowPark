-- Databricks notebook source
CREATE OR REPLACE TABLE STAGING_STORES (store_id varchar(5), location varchar(100), value integer)
;

CREATE TABLE FINAL_STORES (store_id varchar(5), location varchar(100), value integer, start_date date, end_date date, active_flag char)
;

insert into FINAL_STORES (store_id , location , value, start_date, end_date, active_flag ) values ('A0001', 'Lodon', 1000, '2023-01-01', '9999-12-31', 'Y');

truncate table FINAL_STORES;

select * from FINAL_STORES;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = GZIP
;

CREATE STAGE CSV_STAGE
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = CSV_FORMAT
;

--put file://D:\Datasets\stores.csv @CSV_STAGE;

SELECT T.$1, T.$2, T.$3 FROM @CSV_STAGE (FILE_FORMAT => 'CSV_FORMAT') T;

INSERT INTO STAGING_STORES SELECT T.$1, T.$2, T.$3 FROM @CSV_STAGE (FILE_FORMAT => 'CSV_FORMAT') T;

SELECT * FROM STAGING_STORES;








