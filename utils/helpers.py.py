# Databricks notebook source
import yaml
import uuid
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType



def get_config(env):
    with open("/Workspace/Users/yash.karda.yk@gmail.com/commerce-data-platform/config/pipeline_config.yml","r") as f:
        config = yaml.safe_load(f)

    return {
    "base_path": config[env]["base_path"],
    "layers": config["layers"]
    }

def read_delta(spark,path):
    df = spark.read.format("delta").load(path)
    return df

def write_delta(df, path, mode, partition_col=None):
   if partition_col:
        df.write.format("delta").mode(mode).partitionBy(partition_col).save(path)
    else:
        df.write.format("delta").mode(mode).save(path)

def log_run(spark, notebook_name, environment, run_mode, start_time, end_time, rows_processed, status, error_message=None):
    run_id = str(uuid.uuid4())
    data = [(run_id,notebook_name,environment,run_mode,start_time,end_time,rows_processed,status,error_message)]
    schema = StructType([
    StructField("run_id", StringType()),
    StructField("notebook_name", StringType()),
    StructField("environment", StringType()),
    StructField("run_mode", StringType()),
    StructField("start_time", TimestampType()),
    StructField("end_time", TimestampType()),
    StructField("rows_processed", IntegerType()),
    StructField("status", StringType()),
    StructField("error_message", StringType()),
    ])
    df = spark.createDataFrame(data,schema)
    write_delta(df,"/Volumes/workspace/default/ecommerce/audit/pipeline_runs","append")

