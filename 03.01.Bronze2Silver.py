# Databricks notebook source
dbutils.widgets.text("JOB_ID", "1234")
getJOB_ID=dbutils.widgets.get("JOB_ID")

# COMMAND ----------

from datetime import datetime
import pytz
from pyspark.sql.functions import lit
from datetime import datetime
import numpy as np

# COMMAND ----------

# %sql
# truncate table ot_cat_dev.silver.emp_dept 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ot_cat_dev.silver.emp_dept 

# COMMAND ----------

def updExistingRecord():
    df_Rec=spark.sql("""update ot_cat_dev.silver.emp_dept 
    set
    ACTIVE_IND='N',
    EFFECT_END_DT=CURRENT_TIMESTAMP,
    LAST_UPDATED_DT=CURRENT_TIMESTAMP,
    UPDATED_BY='03.01.Bronze2Silver'
    where EMP_DEPT_ID in (
    select emp_dept.EMP_DEPT_ID
    from (
    (select empx.EMPLOYEE_ID,
empx.FIRST_NAME,
empx.LAST_NAME,
empx.EMAIL,
empx.PHONE_NUMBER,
empx.HIRE_DATE,
empx.JOB_ID,
empx.SALARY,
empx.MAX_SALARY,
empx.COMMISSION_PCT,
empx.MANAGER_ID,
empx.DEPARTMENT_ID,
dept.DEPARTMENT_NAME from 
    (select  emp.EMPLOYEE_ID,
emp.FIRST_NAME,
emp.LAST_NAME,
emp.EMAIL,
emp.PHONE_NUMBER,
emp.HIRE_DATE,
emp.JOB_ID,
emp.SALARY,
maxsal.MAX_SALARY,
emp.COMMISSION_PCT,
emp.MANAGER_ID,
emp.DEPARTMENT_ID
 from ot_cat_dev.bronze_ksa.employees emp
    left join (select DEPARTMENT_ID,MAX(SALARY) MAX_SALARY from ot_cat_dev.prebronze_ksa.employees
    group by DEPARTMENT_ID) maxsal
    on emp.DEPARTMENT_ID=maxsal.DEPARTMENT_ID) empx
    left join ot_cat_dev.bronze_ksa.departments dept
    on empx.DEPARTMENT_ID=dept.DEPARTMENT_ID) empdx
    left join (select * from ot_cat_dev.silver.emp_dept  where ACTIVE_IND='Y') emp_dept
    ON empdx.EMPLOYEE_ID=emp_dept.EMPLOYEE_ID
    ) where emp_dept.EMPLOYEE_ID IS NOT NULL
    )"""
    )
    return df_Rec;

# COMMAND ----------

def insExistingRecord():
    df_Rec=spark.sql("""insert into ot_cat_dev.silver.emp_dept 
(
EMPLOYEE_ID,
FIRST_NAME,
LAST_NAME,
EMAIL,
PHONE_NUMBER,
HIRE_DATE,
JOB_ID,
SALARY,
MAX_SALARY,
COMMISSION_PCT,
MANAGER_ID,
DEPARTMENT_ID,
DEPARTMENT_NAME,
ACTIVE_IND,
EFFECT_START_DT,
EFFECT_END_DT,
LAST_CREATED_DT,
CREATED_BY,
LAST_UPDATED_DT,
UPDATED_BY) 
                        select empdx.EMPLOYEE_ID, empdx.FIRST_NAME, empdx.LAST_NAME, empdx.EMAIL, empdx.PHONE_NUMBER, empdx.HIRE_DATE, empdx.JOB_ID, empdx.SALARY, empdx.MAX_SALARY, empdx.COMMISSION_PCT, empdx.MANAGER_ID, empdx.DEPARTMENT_ID, empdx.DEPARTMENT_NAME,'Y' ACTIVE_IND,
    CURRENT_TIMESTAMP EFFECT_START_DT,
    NULL EFFECT_END_DT,
    CURRENT_TIMESTAMP LAST_CREATED_DT,
    '03.01.Bronze2Silver' CREATED_BY,
    NULL LAST_UPDATED_DT,
    NULL UPDATED_BY
    from (
    (select empx.EMPLOYEE_ID,
empx.FIRST_NAME,
empx.LAST_NAME,
empx.EMAIL,
empx.PHONE_NUMBER,
empx.HIRE_DATE,
empx.JOB_ID,
empx.SALARY,
empx.MAX_SALARY,
empx.COMMISSION_PCT,
empx.MANAGER_ID,
empx.DEPARTMENT_ID,
dept.DEPARTMENT_NAME from 
    (select  emp.EMPLOYEE_ID,
emp.FIRST_NAME,
emp.LAST_NAME,
emp.EMAIL,
emp.PHONE_NUMBER,
emp.HIRE_DATE,
emp.JOB_ID,
emp.SALARY,
maxsal.MAX_SALARY,
emp.COMMISSION_PCT,
emp.MANAGER_ID,
emp.DEPARTMENT_ID
 from ot_cat_dev.bronze_ksa.employees emp
    left join (select DEPARTMENT_ID,MAX(SALARY) MAX_SALARY from ot_cat_dev.prebronze_ksa.employees
    group by DEPARTMENT_ID) maxsal
    on emp.DEPARTMENT_ID=maxsal.DEPARTMENT_ID) empx
    left join ot_cat_dev.bronze_ksa.departments dept
    on empx.DEPARTMENT_ID=dept.DEPARTMENT_ID) empdx
    left join ot_cat_dev.silver.emp_dept  emp_dept
    ON empdx.EMPLOYEE_ID=emp_dept.EMPLOYEE_ID
    ) where emp_dept.EMPLOYEE_ID is not null"""
    )
    return df_Rec;

# COMMAND ----------

def insNewRecord():
    df_Rec=spark.sql("""insert into ot_cat_dev.silver.emp_dept  (
EMPLOYEE_ID,
FIRST_NAME,
LAST_NAME,
EMAIL,
PHONE_NUMBER,
HIRE_DATE,
JOB_ID,
SALARY,
MAX_SALARY,
COMMISSION_PCT,
MANAGER_ID,
DEPARTMENT_ID,
DEPARTMENT_NAME,
ACTIVE_IND,
EFFECT_START_DT,
EFFECT_END_DT,
LAST_CREATED_DT,
CREATED_BY,
LAST_UPDATED_DT,
UPDATED_BY)
                        select empdx.EMPLOYEE_ID, empdx.FIRST_NAME, empdx.LAST_NAME, empdx.EMAIL, empdx.PHONE_NUMBER, empdx.HIRE_DATE, empdx.JOB_ID, empdx.SALARY, empdx.MAX_SALARY, empdx.COMMISSION_PCT, empdx.MANAGER_ID, empdx.DEPARTMENT_ID, empdx.DEPARTMENT_NAME,'Y' ACTIVE_IND,
    CURRENT_TIMESTAMP EFFECT_START_DT,
    NULL EFFECT_END_DT,
    CURRENT_TIMESTAMP LAST_CREATED_DT,
    '03.01.Bronze2Silver' CREATED_BY,
    NULL LAST_UPDATED_DT,
    NULL UPDATED_BY
    from (
    (select empx.EMPLOYEE_ID,
empx.FIRST_NAME,
empx.LAST_NAME,
empx.EMAIL,
empx.PHONE_NUMBER,
empx.HIRE_DATE,
empx.JOB_ID,
empx.SALARY,
empx.MAX_SALARY,
empx.COMMISSION_PCT,
empx.MANAGER_ID,
empx.DEPARTMENT_ID,
dept.DEPARTMENT_NAME from 
    (select emp.EMPLOYEE_ID,
emp.FIRST_NAME,
emp.LAST_NAME,
emp.EMAIL,
emp.PHONE_NUMBER,
emp.HIRE_DATE,
emp.JOB_ID,
emp.SALARY,
maxsal.MAX_SALARY,
emp.COMMISSION_PCT,
emp.MANAGER_ID,
emp.DEPARTMENT_ID
 from ot_cat_dev.bronze_ksa.employees emp
    left join (select DEPARTMENT_ID,MAX(SALARY) MAX_SALARY from ot_cat_dev.prebronze_ksa.employees
    group by DEPARTMENT_ID) maxsal
    on emp.DEPARTMENT_ID=maxsal.DEPARTMENT_ID) empx
    left join ot_cat_dev.bronze_ksa.departments dept
    on empx.DEPARTMENT_ID=dept.DEPARTMENT_ID) empdx
    left join ot_cat_dev.silver.emp_dept  emp_dept
    ON empdx.EMPLOYEE_ID=emp_dept.EMPLOYEE_ID
    ) where emp_dept.EMPLOYEE_ID is null"""
    )
    return df_Rec;

# COMMAND ----------

startTime=datetime.now(tz=pytz.timezone('US/Eastern'))
try:
    df_updExistingRecord=updExistingRecord()
    
except Exception as e:
    print(e)
    outputtoADF=f"{e}"
    return_code='FAIL'
    dbutils.notebook.exit(outputtoADF)

endTime=datetime.now(pytz.timezone('US/Eastern'))
execution_time=endTime-startTime
rejected_rows=0
num_updated_rows=df_updExistingRecord.first()['num_affected_rows']
num_deleted_rows=0
num_inserted_rows=0

df_updExistingRecord=df_updExistingRecord.withColumn('num_inserted_rows', lit(num_inserted_rows)).withColumn('num_updated_rows', lit(num_updated_rows)).withColumn('num_deleted_rows', lit(num_deleted_rows)).withColumn('rejected_rows', lit(rejected_rows)).withColumn('rejected_rows', lit(rejected_rows)).withColumn('JOB_ID', lit(getJOB_ID)).withColumn('JOB_NAME', lit('03.01.Bronze2Silver')).withColumn('TRGT_TABLE', lit('ot_cat_dev.silver.emp_dept')).withColumn('INCR_START_DATE', lit(startTime.strftime('%Y-%m-%d %H:%M:%S'))).withColumn('INCR_END_DATE', lit(endTime.strftime('%Y-%m-%d %H:%M:%S'))).withColumn('duration', lit(execution_time.total_seconds()))

df_updExistingRecord.display()

# COMMAND ----------

startTime=datetime.now(tz=pytz.timezone('US/Eastern'))

try:
    df_insExistingRecord=insExistingRecord()
    
except Exception as e:
    print(e)
    outputtoADF=f"{e}"
    return_code='FAIL'
    dbutils.notebook.exit(outputtoADF)


endTime=datetime.now(pytz.timezone('US/Eastern'))
execution_time=endTime-startTime
rejected_rows=df_insExistingRecord.first()['num_affected_rows']-df_insExistingRecord.first()['num_inserted_rows']
num_updated_rows=0
num_deleted_rows=0
num_inserted_rows=df_insExistingRecord.first()['num_inserted_rows']

df_insExistingRecord=df_insExistingRecord.withColumn('num_updated_rows', lit(num_updated_rows)).withColumn('num_deleted_rows', lit(num_deleted_rows)).withColumn('num_inserted_rows', lit(num_inserted_rows)).withColumn('rejected_rows', lit(rejected_rows)).withColumn('rejected_rows', lit(rejected_rows)).withColumn('JOB_ID', lit(getJOB_ID)).withColumn('JOB_NAME', lit('03.01.Bronze2Silver')).withColumn('TRGT_TABLE', lit('ot_cat_dev.silver.emp_dept')).withColumn('INCR_START_DATE', lit(startTime.strftime('%Y-%m-%d %H:%M:%S'))).withColumn('INCR_END_DATE', lit(endTime.strftime('%Y-%m-%d %H:%M:%S'))).withColumn('duration', lit(execution_time.total_seconds()))

df_insExistingRecord.display()

# COMMAND ----------

startTime=datetime.now(tz=pytz.timezone('US/Eastern'))

try:
    df_insNewRecord=insNewRecord()
    
except Exception as e:
    print(e)
    outputtoADF=f"{e}"
    return_code='FAIL'
    dbutils.notebook.exit(outputtoADF)


endTime=datetime.now(pytz.timezone('US/Eastern'))
execution_time=endTime-startTime
rejected_rows=df_insNewRecord.first()['num_affected_rows']-df_insNewRecord.first()['num_inserted_rows']
num_updated_rows=0
num_deleted_rows=0
num_inserted_rows=df_insNewRecord.first()['num_inserted_rows']

df_insNewRecord=df_insNewRecord.withColumn('num_updated_rows', lit(num_updated_rows)).withColumn('num_deleted_rows', lit(num_deleted_rows)).withColumn('num_inserted_rows', lit(num_inserted_rows)).withColumn('rejected_rows', lit(rejected_rows)).withColumn('rejected_rows', lit(rejected_rows)).withColumn('JOB_ID', lit(getJOB_ID)).withColumn('JOB_NAME', lit('03.01.Bronze2Silver')).withColumn('TRGT_TABLE', lit('ot_cat_dev.silver.emp_dept')).withColumn('INCR_START_DATE', lit(startTime.strftime('%Y-%m-%d %H:%M:%S'))).withColumn('INCR_END_DATE', lit(endTime.strftime('%Y-%m-%d %H:%M:%S'))).withColumn('duration', lit(execution_time.total_seconds()))

df_insNewRecord.display()

# COMMAND ----------

df_loadStat=df_updExistingRecord.union(df_insExistingRecord).union(df_insNewRecord)
display(df_loadStat)

# COMMAND ----------

from pyspark.sql.functions import sum, min, max, date_format

df_loadStat_consolidated = df_loadStat.groupBy('JOB_ID','JOB_NAME', 'TRGT_TABLE').agg(
    sum('num_affected_rows').alias('num_affected_rows'),
    sum('num_updated_rows').alias('num_updated_rows'),
    sum('num_deleted_rows').alias('num_deleted_rows'),
    sum('num_inserted_rows').alias('num_inserted_rows'),
    sum('rejected_rows').alias('rejected_rows'),
    sum('duration').alias('duration'),
    min(date_format('INCR_START_DATE', 'yyyy-MM-dd HH:mm:ss')).alias('INCR_START_DATE'),
    max(date_format('INCR_END_DATE', 'yyyy-MM-dd HH:mm:ss')).alias('INCR_END_DATE')
)

display(df_loadStat_consolidated)

# COMMAND ----------

arr = np.array(df_loadStat_consolidated.collect())

# COMMAND ----------

arr

# COMMAND ----------

# MAGIC %sql
# MAGIC --truncate table ot_cat_dev.silver.emp_dept;
# MAGIC select * from ot_cat_dev.silver.emp_dept ;

# COMMAND ----------

dbutils.notebook.exit(arr)