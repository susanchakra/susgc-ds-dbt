# Databricks notebook source
dbutils.widgets.text(name="job_name", defaultValue="default", label="Job_name")
dbutils.widgets.text(name="task_name", defaultValue="default", label="Task_name")

# COMMAND ----------

v_job_name = dbutils.widgets.get("job_name")
v_task_name = dbutils.widgets.get("task_name")

# COMMAND ----------

from datetime import datetime
from pytz import timezone
from pyspark.sql.functions import current_timestamp

# notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# notebook_name = notebook_path.split('/')[-1]

est = timezone('US/Eastern')
curr_datetime = datetime.now(est).strftime("%Y_%m_%dT%H_%M_%S")





# COMMAND ----------

import logging
import json
import os

class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            'timestamp': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'message': record.getMessage()
        }
        return json.dumps(log_record)

# COMMAND ----------

# Create a logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set your logging level

# COMMAND ----------

# Create a file handler
log_file_name=f'{v_job_name}__{v_task_name}_{curr_datetime}.log'
log_file_path = f'/tmp/{log_file_name}'
handler = logging.FileHandler(log_file_path)

# COMMAND ----------

# Set the formatter for the handler
formatter = JSONFormatter(datefmt='%Y-%m-%d %H:%M:%S')
handler.setFormatter(formatter)

# COMMAND ----------

# Add the handler to the logger
logger.addHandler(handler)

# COMMAND ----------

# Debug statement to check if the file is created
if os.path.exists(log_file_path):
    print(f"Log file created at: {log_file_path}")
else:
    print(f"Failed to create log file at: {log_file_path}")

# COMMAND ----------

# Log messages
# logger.info('Test log message')
# logger.error('Error message')

# COMMAND ----------

# # Debug to check the content of the log file
# with open(log_file_path, 'r') as file:
#     log_content = file.read()
#     print(f"Log file content:\n{log_content}")

# COMMAND ----------

# logging.shutdown()