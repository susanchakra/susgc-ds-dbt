# Databricks notebook source
# MAGIC %md
# MAGIC ###This notebook extracts Employee data from PreBronze and loads into Bronze

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime
from pytz import timezone
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import lit
import pytz

# COMMAND ----------

# MAGIC %run /Workspace/Notebook/Project_1_ER_Modernization/00.01.GlobalFunctions
# MAGIC

# COMMAND ----------

# MAGIC %run /Workspace/Notebook/Project_1_ER_Modernization/00.02.SetStructuredLogging

# COMMAND ----------

dbutils.widgets.text(name="job_id", defaultValue="0000",label= "Job_id")
dbutils.widgets.text(name="job_run_id", defaultValue="0000",label= "Job_run_id")
dbutils.widgets.text(name="task_run_id", defaultValue="0000",label= "task_run_id")
dbutils.widgets.text(name="job_name", defaultValue="defaultValue",label= "job_name")
dbutils.widgets.text(name="task_name", defaultValue="defaultValue",label= "task_name")
dbutils.widgets.text(name="app_id", defaultValue="00",label= "App_id")
dbutils.widgets.text(name="entity_id", defaultValue="0",label= "Entity_id")
dbutils.widgets.text(name="environment", defaultValue="defaultValue",label= "Environment")
dbutils.widgets.text(name="source_table", defaultValue="defaultValue",label= "Source_Table")
dbutils.widgets.text(name="target_table", defaultValue="defaultValue",label= "Target_Table")
dbutils.widgets.text(name="is_delschema", defaultValue="N",label= "is_DelSchema")


# COMMAND ----------

v_job_id=dbutils.widgets.get("job_id")
v_job_run_id=dbutils.widgets.get("job_run_id")
v_task_run_id=dbutils.widgets.get("task_run_id")
v_job_name=dbutils.widgets.get("job_name")
v_task_name=dbutils.widgets.get("task_name")
v_app_id=dbutils.widgets.get("app_id")
v_entity_id=dbutils.widgets.get("entity_id")
env=dbutils.widgets.get("environment")
v_source_table=dbutils.widgets.get("source_table")
v_target_table=dbutils.widgets.get("target_table")
v_is_delschema=dbutils.widgets.get("is_delschema")

# COMMAND ----------

bronzetableName=v_target_table
prebronzetableName=v_source_table
prebronzeDelTableName=prebronzetableName.replace("dbo", "del")


# COMMAND ----------


logger.info(f'Starting job for app_id {v_app_id} and entity_id {v_entity_id}')


# COMMAND ----------

logger.info(f'param key job_id: {v_job_id}')
logger.info(f'param key job_run_id: {v_job_run_id}')
logger.info(f'param key task_run_id: {v_task_run_id}')
logger.info(f'param key job_name: {v_job_name}')
logger.info(f'param key task_name: {v_task_name}')
logger.info(f'param key app_id: {v_app_id}')
logger.info(f'param key entity_id: {v_entity_id}')
logger.info(f'param key environment: {env}')
logger.info(f'param key source_table: {v_source_table}')
logger.info(f'param key target_table: {v_target_table}')
logger.info(f'param key is_delschema: {v_is_delschema}')

# COMMAND ----------

v_job_id=dbutils.widgets.get("job_id")
v_job_run_id=dbutils.widgets.get("job_run_id")
v_task_run_id=dbutils.widgets.get("task_run_id")
v_job_name=dbutils.widgets.get("job_name")
v_task_name=dbutils.widgets.get("task_name")
v_app_id=dbutils.widgets.get("app_id")
v_entity_id=dbutils.widgets.get("entity_id")
env=dbutils.widgets.get("environment")
v_source_table=dbutils.widgets.get("source_table")
v_target_table=dbutils.widgets.get("target_table")
v_is_delschema=dbutils.widgets.get("is_delschema")

# COMMAND ----------

def loadBronzeEmployees(app_id,entity_id,is_delschema):
      incrementalStartDate=getIncrementalStartDate(v_task_name,app_id, entity_id)
      incrementalEndDate=getIncrementalEndDate(v_task_name,app_id, entity_id)
      if is_delschema.upper()=='Y':
            query=f"""
      insert overwrite {bronzetableName}
      (
      select t.*,'N' as IS_DELETED,
      current_timestamp() as INGESTION_TIME,
      'System' as INGESTION_ID  from {prebronzetableName} t where app_id='{app_id}' and entity_id='{entity_id}' and LAST_UPDATED_DT between '{incrementalStartDate}' and '{incrementalEndDate}'
      union
      select t.*,'Y' as IS_DELETED,
      current_timestamp() as INGESTION_TIME,
      'System' as INGESTION_ID  from {prebronzeDelTableName} t where app_id='{app_id}' and entity_id='{entity_id}' and LAST_UPDATED_DT between '{incrementalStartDate}' and '{incrementalEndDate}'
      ) """
      else:
            query=f"""
            insert overwrite {bronzetableName}
            (
            select t.*,'N' as IS_DELETED,
            current_timestamp() as INGESTION_TIME,
            'System' as INGESTION_ID  from {prebronzetableName} t where app_id='{app_id}' and entity_id='{entity_id}' and LAST_UPDATED_DT between '{incrementalStartDate}' and '{incrementalEndDate}'
             ) """

      try:
            logger.info(f'Executing query: {query}')            #log Query
            startDateTime=datetime.now(tz=pytz.timezone('US/Eastern'))
            
            spark.sql(query)
            
            endDateTime=datetime.now(tz=pytz.timezone('US/Eastern'))
            insRecord=collectOperationalMetrics(bronzetableName,'INSERT') #collect operational metrics
            logger.info(f'Number of rows inserted: {insRecord}') 
            updateJobLoadStat(v_job_id,v_job_run_id,v_task_run_id,v_job_name,v_task_name,bronzetableName,startDateTime,endDateTime,insRecord,0,0,0,'SUCCEEDED',app_id, entity_id)           
       
            logger.info('Setting job status as SUCCEEDED')            #log information
            setJobStatus(v_task_name,app_id, entity_id, 'SUCCEEDED')
      except Exception as e:
            logger.error(f'Error message {e}')
            logger.info('Setting job status as FAILED')  
            setJobStatus(v_task_name,app_id, entity_id, 'FAILED')
            updateJobError(v_job_id, v_job_run_id,v_task_run_id,v_job_name,v_task_name,prebronzetableName, datetime.now(tz=pytz.timezone('US/Eastern')),str(e.getSqlState()), str(e.getMessage()), 'Errors', 'Exception', app_id, entity_id)
            #raise Exception(e)
            dbutils.notebook.exit(f"Error: {e}")


# COMMAND ----------

setIncrementalStartEndDate(v_task_name,v_app_id, v_entity_id)
loadBronzeEmployees(v_app_id,v_entity_id,v_is_delschema)

# COMMAND ----------

if os.path.exists(log_file_path):
    dbutils.fs.mv(f'file:/tmp/{log_file_name}', f'abfss://log@storageopentwinsdev.dfs.core.windows.net/Project_1_ER_Modernization/{log_file_name}')

logging.shutdown()


# COMMAND ----------

# %sql
# DESCRIBE HISTORY ot_cat_dev.bronze_emea.employees

# COMMAND ----------

# %sql
#         SELECT operationMetrics.numOutputRows
#         FROM (DESCRIBE HISTORY ot_cat_dev.bronze_emea.employees)
#         WHERE operation = 'WRITE' -- or 'MERGE', 'INSERT', etc.
#         ORDER BY version DESC
#         LIMIT 1;

# COMMAND ----------

# %sql
# select * from system.lakeflow.job_run_timeline

# COMMAND ----------

# %sql
# select * from system.lakeflow.job_task_run_timeline