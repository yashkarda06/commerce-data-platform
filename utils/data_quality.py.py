# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F
def check_row_count(df, table_name, min_rows=1):
    count = df.count()
 
    if count < min_rows:
        raise Exception(f"{table_name} has {count} rows, expected at least {min_rows}")
    else:
        print(f"{table_name} row count check passed: {count} rows")
   
def check_no_duplicates(df, table_name, primary_key):
    total_count = df.count()
    distinct_count = df.select(primary_key).distinct().count()
    if total_count !=  distinct_count:
        raise Exception(f"{table_name} has {total_count} rows, expected at unique {distinct_count}")
    else:
        print(f"{table_name} duplicate check passed: {distinct_count} rows")
     
def check_no_nulls(df, table_name, critical_columns):
    for column in critical_columns:
        count = df.filter(F.col(column).isNull()).count()
        if count > 0:
            raise Exception(f"{table_name} has column: {column} with {count} null values")

    print(f"{table_name} null check passed for all critical columns")
    
def check_value_range(df, table_name, column, min_val, max_val):
    count = df.filter((F.col(column) <  min_val) | (F.col(column) > max_val)).count()
    if count > 0:
        raise Exception(f"{table_name} has column: {column} with {count} out of range values")

    print(f"{table_name} range check passed for all critical columns")

def run_all_checks(df, table_name, primary_key, critical_columns):
    check_row_count(df, table_name)
    check_no_duplicates(df, table_name, primary_key)
    check_no_nulls(df, table_name, critical_columns)
    print(f"All checks passed for {table_name}")
     
