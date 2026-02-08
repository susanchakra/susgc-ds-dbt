-- Databricks notebook source
select * from f_source001_dev.dbo.departments_emea

-- COMMAND ----------

select * from f_source001_dev.del.departments_emea

-- COMMAND ----------

select * from f_source001_dev.dbo.employees_emea

-- COMMAND ----------

select * from f_source001_dev.del.employees_emea

-- COMMAND ----------

select * from ot_cat_dev.silver.emp_dept

-- COMMAND ----------

truncate table ot_cat_dev.silver.emp_dept

-- COMMAND ----------

alter table ot_cat_dev.silver.emp_dept
add column APP_ID integer,	ENTITY_ID integer

-- COMMAND ----------

ALTER TABLE ot_cat_dev.silver.emp_dept ALTER COLUMN APP_ID SET NOT NULL;

-- COMMAND ----------

ALTER TABLE ot_cat_dev.silver.emp_dept ALTER COLUMN ENTITY_ID SET NOT NULL;

-- COMMAND ----------

select * from ot_cat_dev.bronze_ksa.job_load_stat

-- COMMAND ----------

Create or replace table ot_cat_dev.silver.JOB_LOAD_CTRL(
JOB_NAME varchar(100),
LAST_JOB_STATUS varchar(100),
INCR_START_DATE timestamp,
INCR_END_DATE timestamp,
CREATED_DATE_TIME date,
CREATED_BY varchar(100),
UPDATED_DATE_TIME date,
UPDATED_BY varchar(100),
APP_ID integer,	
ENTITY_ID integer
)


-- COMMAND ----------

truncate table ot_cat_dev.silver.JOB_LOAD_CTRL

-- COMMAND ----------


update ot_cat_dev.silver.JOB_LOAD_CTRL set INCR_END_DATE =
current_timestamp() - INTERVAL 365 DAYS

-- COMMAND ----------

TRUNCATE TABLE ot_cat_dev.silver.JOB_LOAD_CTRL

-- COMMAND ----------

insert into ot_cat_dev.silver.JOB_LOAD_CTRL values 
('PreBronze2BronzeEmployee_EMEA_1','SUCCEEDED',current_timestamp(),current_timestamp(),current_date(),'SYSTEM',current_date(),'SYSTEM',62,1);
insert into ot_cat_dev.silver.JOB_LOAD_CTRL values 
('PreBronze2BronzeEmployee_EMEA_2','SUCCEEDED',current_timestamp(),current_timestamp(),current_date(),'SYSTEM',current_date(),'SYSTEM',62,2);
insert into ot_cat_dev.silver.JOB_LOAD_CTRL values 
('PreBronze2BronzeEmployee_EMEA_3','SUCCEEDED',current_timestamp(),current_timestamp(),current_date(),'SYSTEM',current_date(),'SYSTEM',62,3);

insert into ot_cat_dev.silver.JOB_LOAD_CTRL values 
('PreBronze2BronzeDepartment_EMEA_1','SUCCEEDED',current_timestamp(),current_timestamp(),current_date(),'SYSTEM',current_date(),'SYSTEM',62,1);
insert into ot_cat_dev.silver.JOB_LOAD_CTRL values 
('PreBronze2BronzeDepartment_EMEA_2','SUCCEEDED',current_timestamp(),current_timestamp(),current_date(),'SYSTEM',current_date(),'SYSTEM',62,2);
insert into ot_cat_dev.silver.JOB_LOAD_CTRL values 
('PreBronze2BronzeDepartment_EMEA_3','SUCCEEDED',current_timestamp(),current_timestamp(),current_date(),'SYSTEM',current_date(),'SYSTEM',62,3);

-- COMMAND ----------

select * from ot_cat_dev.silver.JOB_LOAD_CTRL

-- COMMAND ----------

Create or replace table ot_cat_dev.silver.JOB_ERROR(
JOB_ID STRING,
JOB_RUN_ID string,
TASK_RUN_ID STRING,
JOB_NAME STRING, 
TASK_NAME STRING, 
SRC_TABLE_NAME varchar(100),
UPDATED_TIME timestamp,
ERROR_CODE varchar(100),
ERROR_DESC STRING,
ERROR_SEVERITY varchar(100),
ERROR_TYPE varchar(100),
APP_ID integer,	
ENTITY_ID integer
)

-- COMMAND ----------

create or replace table ot_cat_dev.silver.JOB_LOAD_STAT
(
JOB_ID STRING,
JOB_RUN_ID string,
TASK_RUN_ID STRING, 
JOB_NAME STRING,
TASK_NAME STRING,
TRGT_TABLE varchar(100),
START_DATE_TIME timestamp,
END_DATE_TIME timestamp,
INS_RECORD integer, 
UPD_RECORD integer, 
DEL_RECORD integer, 
REJ_RECORD integer,
STATUS varchar(100),
APP_ID integer,	
ENTITY_ID integer
)

-- COMMAND ----------

SHOW CREATE TABLE ot_cat_dev.prebronze_emea.departments

-- COMMAND ----------

CREATE TABLE ot_cat_dev.bronze_nasa.employees (
  EMPLOYEE_ID INT,
  FIRST_NAME STRING,
  LAST_NAME STRING,
  EMAIL STRING,
  PHONE_NUMBER STRING,
  HIRE_DATE STRING,
  JOB_ID STRING,
  SALARY INT,
  COMMISSION_PCT STRING,
  MANAGER_ID STRING,
  DEPARTMENT_ID INT,
  LAST_UPDATED_DT TIMESTAMP,
  APP_ID INT,
  ENTITY_ID INT,
  IS_DELETED STRING,
  INGESTION_TIME TIMESTAMP,
  INGESTION_ID STRING)
USING delta
PARTITIONED BY (APP_ID, ENTITY_ID)

-- COMMAND ----------

CREATE TABLE ot_cat_dev.bronze_nasa.departments (
  DEPARTMENT_ID INT,
  DEPARTMENT_NAME STRING,
  MANAGER_ID STRING,
  LOCATION_ID INT,
  LAST_UPDATED_DT TIMESTAMP,
  APP_ID INT,
  ENTITY_ID INT,
  IS_DELETED STRING,
  INGESTION_TIME TIMESTAMP,
  INGESTION_ID STRING)
USING delta
PARTITIONED BY (APP_ID, ENTITY_ID)

-- COMMAND ----------

update ot_cat_dev.silver.JOB_LOAD_CTRL set INCR_END_DATE ='2025-04-04 03:58:16.645009'
where Job_name='PreBronze2BronzeEmployee_EMEA_1'

-- COMMAND ----------

update ot_cat_dev.silver.JOB_LOAD_CTRL set INCR_END_DATE ='2025-04-04 03:58:16.645009'
where Job_name='PreBronze2BronzeDepartment_EMEA_1'

-- COMMAND ----------

select * from ot_cat_dev.silver.JOB_LOAD_CTRL
where Job_name in ('PreBronze2BronzeEmployee_EMEA_1','PreBronze2BronzeDepartment_EMEA_1')


-- COMMAND ----------

select * from ot_cat_dev.bronze_emea.employees

-- COMMAND ----------

select * from ot_cat_dev.bronze_emea.departments

-- COMMAND ----------

-- truncate table ot_cat_dev.silver.job_load_stat

-- COMMAND ----------

select * from ot_cat_dev.silver.job_load_stat


-- COMMAND ----------

select * from ot_cat_dev.silver.JOB_ERROR