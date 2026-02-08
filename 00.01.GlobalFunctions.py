# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp


# COMMAND ----------

dbutils.widgets.text("Environment", "dev")

# COMMAND ----------

env=dbutils.widgets.get("Environment")

# COMMAND ----------

df=spark.sql(f"use catalog ot_cat_{env}")


# COMMAND ----------

def getIncrementalStartDate(jobName, appID, entityID):
    try:
     df = spark.sql(f"select max(INCR_START_DATE) as INCR_START_DATE from silver.JOB_LOAD_CTRL where APP_ID = '{appID}' and ENTITY_ID = '{entityID}' and JOB_NAME = '{jobName}'")
     return df.first().INCR_START_DATE
    except Exception as e:
        print(e)

# COMMAND ----------

def getIncrementalEndDate(jobName, appID, entityID):
    try:
     df = spark.sql(f"select max(INCR_END_DATE) as INCR_END_DATE from silver.JOB_LOAD_CTRL where APP_ID = '{appID}' and ENTITY_ID = '{entityID}' and JOB_NAME = '{jobName}'")
     return df.first().INCR_END_DATE
    except Exception as e:
        print(e)

# COMMAND ----------

def setIncrementalStartEndDate(jobName, appID, entityID):
    try:
     df = spark.sql(f"""
                    Update silver.JOB_LOAD_CTRL 
                        set INCR_START_DATE = INCR_END_DATE,
                        INCR_END_DATE = current_timestamp(),
                        UPDATED_DATE_TIME = current_timestamp()
                        where APP_ID = '{appID}' and ENTITY_ID = '{entityID}' and JOB_NAME = '{jobName}' and LAST_JOB_STATUS = 'SUCCEEDED'
                    """)
    except Exception as e:
        print(e)

# COMMAND ----------

def setJobStatus(jobName, appID, entityID, status):
    try:
     df = spark.sql(f"""
                    Update silver.JOB_LOAD_CTRL 
                    set LAST_JOB_STATUS = '{status}',
                    UPDATED_DATE_TIME = current_timestamp()
                    where APP_ID = '{appID}' and ENTITY_ID = '{entityID}' and JOB_NAME = '{jobName}'
                    """)
    except Exception as e:
        print(e)

# COMMAND ----------

def collectOperationalMetrics(tableName,operationName):
    try:    
        delta_table = DeltaTable.forName(spark, tableName)
        history_df = delta_table.history()
        operationName=operationName.upper()
        if operationName == 'INSERT':
            return history_df.select(F.col('operationMetrics')).collect()[0].operationMetrics['numOutputRows']
        elif operationName == 'UPDATE':
            return history_df.select(F.col('operationMetrics')).collect()[0].operationMetrics['numUpdatedRows']
        elif operationName == 'DELETE':
            return history_df.select(F.col('operationMetrics')).collect()[0].operationMetrics['numRemovedRows']
    except Exception as e:
        print(f'Error message: {e}')

# COMMAND ----------

def updateJobLoadStat(jobID,jobRunID,taskRunID,jobName,taskName,tableName,startDateTime,endDateTime,insRecord,updRecord,delRecord,rejRecord,status,appID,entityID):
    try:
        spark.sql(f"""
                    insert into ot_cat_dev.silver.job_load_stat(JOB_ID,JOB_RUN_ID,TASK_RUN_ID,JOB_NAME,TASK_NAME,TRGT_TABLE,START_DATE_TIME,END_DATE_TIME,INS_RECORD,UPD_RECORD,DEL_RECORD,REJ_RECORD,STATUS,APP_ID,ENTITY_ID) values ('{jobID}','{jobRunID}','{taskRunID}','{jobName}','{taskName}','{tableName}','{startDateTime}','{endDateTime}','{insRecord}','{updRecord}','{delRecord}','{rejRecord}','{status}','{appID}','{entityID}')
                    """)
    except Exception as e:
        print(f'Error message: {e}')
              
              

# COMMAND ----------

def updateJobError(jobID,jobRunID,taskRunID,jobName,taskName,srcTableName,updatedTime,errorCode,errorDesc,errorSeverity,errorType,appID,entityID):
    try:
        spark.sql(f"""
                  insert into ot_cat_dev.silver.JOB_ERROR(JOB_ID,JOB_RUN_ID,TASK_RUN_ID,JOB_NAME,TASK_NAME,SRC_TABLE_NAME,UPDATED_TIME,ERROR_CODE,ERROR_DESC,ERROR_SEVERITY,ERROR_TYPE,APP_ID,ENTITY_ID) values ('{jobID}','{jobRunID}','{taskRunID}','{jobName}','{taskName}','{srcTableName}','{updatedTime}','{errorCode}','{errorDesc}','{errorSeverity}','{errorType}','{appID}','{entityID}')
                    """)
    except Exception as e:
        print(f'Error message from updateJobError function: {e}')
