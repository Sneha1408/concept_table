from airflow.utils.task_group import TaskGroup
import configparser
import json
from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
from qcds.utility.qcds_utility import Utility
from pathlib import Path
from sqlalchemy import create_engine
import os
import ast
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from urllib.parse import quote_plus

FILE_DIR = Path(__file__).resolve()
configpath = FILE_DIR.parents[1].joinpath("config", "testconfig.ini")
config = configparser.ConfigParser()
config.read(configpath)

# Define the chunk size
CHUNK_SIZE = 1000
full_logger_name = os.path.basename(__file__)
logger_name = os.path.splitext(full_logger_name)[0]
utils = Utility(configpath, logger_name)
logger = utils.logger
concept_table = str(config.get("tables", "concept_table"))

# mssql connection
mssql_server = utils.properties['mssql_server']
mssql_database = utils.properties['mssql_database']
mssql_user = utils.properties['mssql_user']
mssql_password = utils.properties['mssql_password']
mssql_password = quote_plus(mssql_password)
mssql_engine_str = f"mssql+pymssql://{mssql_user}:{mssql_password}@{mssql_server}/{mssql_database}"
mssql_engine = create_engine(mssql_engine_str)

# postgresql connection
postgresql_server = str(config.get("POSTGRESQL_CONNECTION", "server"))
postgresql_database = str(config.get("POSTGRESQL_CONNECTION", "database"))
postgresql_user = str(config.get("POSTGRESQL_CONNECTION", "user"))
postgresql_password = str(config.get("POSTGRESQL_CONNECTION", "password"))
postgresql_schema = str(config.get("POSTGRESQL_CONNECTION", "schema"))
postgresql_port = '5432'
postgresql_password_encoded = quote_plus(postgresql_password)
postgresql_engine_str = f"postgresql://{postgresql_user}:{postgresql_password_encoded}@{postgresql_server}:{postgresql_port}/{postgresql_database}"
postgresql_engine = create_engine(postgresql_engine_str)


def concept_load():
    try:
        max_concept_id_query = f"SELECT MAX(concept_id) FROM {postgresql_schema}.{concept_table}"
        print(max_concept_id_query)
        max_concept_id_result = postgresql_engine.execute(max_concept_id_query)
        max_concept_id = max_concept_id_result.fetchone()[0]
        # mssql_client = utils.create_or_get_database_mssql()
        # client = utils.create_or_get_database_psql()
        sql_query = """
                        select
                        Insurance_Description as concept_name,
                        'Procedure' as domain_id,
                        'ICD10' as vocabulary_id,
                          'ICD10 code' as concept_class_id,
                        '' as standard_concept,
                           Procedure_Code as concept_code,
                       '1990-01-05' as valid_start_date,
                       '2030-12-01' as valid_end_date,
                        '' as invalid_reason,
                        '' as pm_icd10_code
                        FROM
                         PM_Procedures imt;
        """
        logger.info(sql_query)

        df = pd.read_sql(sql_query, mssql_engine)
        print(df.columns)
        df_rows = len(df)
        range_values = range(max_concept_id + 1, max_concept_id + df_rows + 1, 1)
        df['concept_id'] = range_values
        # range_values = range(1, df_rows + 1, 1)
        # df['concept_id'] = range_values
       # df['concept_code'] = df['concept_code'].apply(lambda x: x.zfill(5))

        col_list = ['concept_id', 'concept_name', 'domain_id', 'vocabulary_id', 'concept_class_id',
                     'standard_concept', 'concept_code', 'valid_start_date', 'valid_end_date', 'invalid_reason']
        
        #df['concept_code'] = df['concept_code'].astype(str)
        df['concept_name'] = df['concept_name'].fillna('Unknown')
        df = df.reindex(columns=col_list)
        
        df.to_csv('concept_table_procedure.csv', index=False)

        df.to_sql(concept_table, con=postgresql_engine,schema=postgresql_schema, if_exists='append', index=False)
        value_at_index_1 = df.loc[14720, 'concept_code']
        print(value_at_index_1)
        print(df)

    except Exception as e:
        logger.error(e)


dag = DAG('concept_load', schedule_interval=None, start_date=datetime(
    year=2022, month=11, day=22), catchup=False)

t1 = PythonOperator(
    task_id='add_data',
    python_callable=concept_load,
    dag=dag
)

t1
