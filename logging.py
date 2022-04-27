# Databricks notebook source
# MAGIC %run ./parallel_processing

# COMMAND ----------
import logging
import datetime
import time
 
file_date = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
logger_dir = '/dbfs/dbfs/tmp/top'
logger_filename = 'top_custom_log'+file_date+'.log'
log_file = logger_dir + logger_filename
logger = logging.getLogger('custom_log')
logger.setLevel(logging.DEBUG)
 
fh = logging.FileHandler(log_file,mode='a')
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
 
#formatter = logging.Formatter('%(asctime)% - %(name)% - %(levelname)% - %(message)%')
#fh.setFormatter(formatter)
#ch.setFormatter(formatter)
 
logger.addHandler(fh)
logger.addHandler(ch)
 
print(log_file)

# COMMAND ----------

logger.info('start info logger')

# COMMAND ----------

logging.shutdown()

# COMMAND ----------
