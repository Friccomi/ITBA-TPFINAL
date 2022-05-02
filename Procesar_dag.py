"""Stocks dag extended."""
import os
from pathlib import Path

from pendulum import YEARS_PER_CENTURY

PYTHONPATH = "PYTHONPATH"
try:
    pythonpath = os.environ[PYTHONPATH]
    print(pythonpath)
except KeyError:
    pythonpath = ""
# print("BEFORE:", pythonpath)
folder = Path(__file__).resolve().parent.joinpath("convert_train")
# print(f"{folder=}")
pathlist = [str(folder)]
if pythonpath:
    pathlist.extend(pythonpath.split(os.pathsep))
#    print(f"{pathlist=}")
os.environ[PYTHONPATH] = os.pathsep.join(pathlist)
# print("AFTER:", os.environ[PYTHONPATH])


from ast import Pass
import json
from datetime import datetime
from os import ctermid
from time import sleep
import numpy as np
import pandas as pd
import requests  # type: ignore

import sqlalchemy


from airflow.providers import postgres
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from sqlPostgresCli import SqlPostgresClient
from decouple import config
import matplotlib.pyplot as plt

import csv
import boto3
import convert_train.s3_conn as conn
from io import StringIO
import sagemaker.amazon.common
import convert_train._convert as conv
import convert_train._train as train
import matplotlib.pyplot as plt
import convert_train._graphics as graf
import convert_train._general as grl
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float

SCHEMA = config("DB_SCHEMA")
print(SCHEMA)

destiny = conn.destiny
origin = conn.origin
if origin == "s3":
    print(conn.AWS_REGION)
    print(conn.AWS_ACCESS_KEY_ID)
    print(conn.AWS_SECRET_ACCESS_KEY)
    print(conn.AWS_SESSION_TOKEN)
    s3 = boto3.resource(
        service_name="s3",
        region_name=conn.AWS_REGION,
        aws_access_key_id=conn.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=conn.AWS_SECRET_ACCESS_KEY,
        aws_session_token=conn.AWS_SESSION_TOKEN,
    )
    bucket_name = conn.AWS_BUCKET_NAME
    bucket = s3.Bucket(bucket_name)
    bucket_obj = s3.Bucket(bucket_name)
    execution_role = conn.AWS_EXECUTION_ROLE
    region = conn.AWS_REGION
    print(bucket_name)
    print(bucket)
else:
    s3 = ""
    bucket_name = ""
    bucket = ""
    region = ""

prefix = "sagemaker/rcf-benchmarks"
path_name = "tp/"


def _clean_s3_dir(path, **context):
    pass


def _calculate_mean_delay(path, **context):
    pass


def _search_unnormals(path, **context):
    pass


def _search_unnormals(path, **context):
    pass


def _generate_graphics(path, **context):
    pass


def _create_delete_table(path, **context):
    pass


def _save_in_DB(path, **context):
    pass


# if __name__ == "__main__":
# _clean_s3_dir("tp/")
#  prefix1 = format(os.getcwd()) + "/tp/"
# _calculate_mean_delay("tp/")
# _search_unnormals("tp/")
# _generate_graphics("tp/")
# _create_delete_table("tp/")
# _save_in_DB("tp/")


default_args = {
    "owner": "flor",
    "retries": 0,
    "start_date": datetime(2011, 1, 1),
    "end_date": datetime(year=2018, month=1, day=1),
}

with DAG(
    "TP-FINAL",
    default_args=default_args,
    schedule_interval="@yearly",
) as dag:
    create_schema = PostgresOperator(
        task_id="Create_schema_if_not_Exists",
        sql="sql/create_env.sql",
        postgres_conn_id=config("CONN_ID"),
    )
    clean_s3_dir = PythonOperator(
        task_id="Clean_s3_dir",
        python_callable=_clean_s3_dir,
        op_kwargs={"path": "tp/"},
        provide_context=True,
    )
    calculate_mean_delay = PythonOperator(
        task_id="Calculate_mean_delays",
        python_callable=_calculate_mean_delay,
        op_kwargs={"path": "tp/"},
        provide_context=True,
    )
    search_unnormals = PythonOperator(
        task_id="Search_unnormals",
        python_callable=_search_unnormals,
        op_kwargs={"path": "tp/"},
        provide_context=True,
    )
    generate_graphics = PythonOperator(
        task_id="Generate_grafics",
        python_callable=_generate_graphics,
        op_kwargs={"path": "tp/"},
        provide_context=True,
    )
    create_delete_table = PythonOperator(
        task_id="Create_delete_table",
        python_callable=_create_delete_table,
        op_kwargs={"path": "tp/"},
        provide_context=True,
    )
    save_in_DB = PythonOperator(
        task_id="Save_in_DB",
        python_callable=_save_in_DB,
        op_kwargs={"path": "tp/"},
        provide_context=True,
    )

    (
        create_schema
        >> clean_s3_dir
        >> calculate_mean_delay
        >> search_unnormals
        >> generate_graphics
        >> create_delete_table
        >> save_in_DB
    )
