from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.models.baseoperator import chain
from airflow.hooks.base_hook import BaseHook
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from sqlalchemy import create_engine
from airflow.providers.postgres.operators.postgres import PostgresOperator
import json

default_args = {
    'owner': 'lluis',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'odata_to_postgres',
    default_args=default_args,
    schedule_interval='@daily',  # Set your desired schedule interval
)

# List of dictionaries containing task information
levels = [
    {"url": "https://analytics.dev.azure.com/GNHearing/_odata/v3.0/WorkItems?%24filter=%28Project%2FProjectName+eq+%27GNH%27+and+WorkItemType+eq+%27Portfolio+Epic%27+and+Area%2FAreaPath+eq+%27GNH%27+and+State+ne+%27Closed%27+and+State+ne+%27Removed%27%29&%24select=WorkItemId%2CWorkItemType%2CTitle%2CState%2CTagNames", "level": 0},
    {"url": "https://analytics.dev.azure.com/GNHearing/_odata/v3.0/WorkItems?%24filter=+Project%2FProjectName+eq+%27GNH%27+and+Parent%2FWorkItemId+ne+null+and+Parent%2FArea%2FAreaPath+eq+%27GNH%27+and+Parent%2FWorkItemType+eq+%27Portfolio+Epic%27+and+Parent%2FState+ne+%27Closed%27+and+Parent%2FState+ne+%27Removed%27&%24select=WorkItemId%2CWorkItemType%2CTitle%2CState%2CCustom_ReasonForMustDo%2CParentWorkItemId%2CGNH_ValueType%2CCustom_RequestforPI%2CCustom_Prioritized%2CCustom_TestAutomationEffort%2CCustom_TestManuelEffort%2CCustom_TestSystemEffort%2CGNH_AppDev_Effort%2CGNH_AppSystem_Effort%2CCustom_LabellingEffort%2CCustom_MarketAccessEffort%2CCustom_RegulatoryCertificationEffort%2CCustom_RegulatoryProductComplianceEffort%2CGNH_FT_Effort%2CGNH_OS_Effort%2CGNH_Sound_Effort%2CGNH_System_Effort%2CGNH_Dev_Effort%2CGNH_QA_Effort%2CCustom_FeatureElectronicIntegration%2CCustom_FeatureRadioSystems%2CCustom_FeatureMaterialsandReliability%2CCustom_FeatureMechanicalDevelopmentCPH%2CCustom_FeatureMechanicalDevelopmentXMN%2CCustom_FeatureAcousticDevelopmentCPH%2CCustom_FeatureAcousticDevelopmentXMN%2CCustom_FeatureQualificationXMN", "level": 1},
    {"url": "https://analytics.dev.azure.com/GNHearing/_odata/v3.0/WorkItems?%24filter=Project%2FProjectName+eq+%27GNH%27+and+Parent%2FWorkItemId+ne+null+and+Parent%2FParent%2FProject%2FProjectName+eq+%27GNH%27+and+Parent%2FParent%2FArea%2FAreaPath+eq+%27GNH%27+and+Parent%2FParent%2FWorkItemType+eq+%27Portfolio+Epic%27+and+Parent%2FParent%2FState+ne+%27Closed%27+and+State+ne+%27Removed%27&%24select=WorkItemId%2CWorkItemType%2CTitle%2CState%2CParentWorkItemId%2C+Custom_ReasonForMustDo%2CGNH_ValueType%2CCustom_RequestforPI%2CCustom_Prioritized%2CCustom_TestAutomationEffort%2CCustom_TestManuelEffort%2CCustom_TestSystemEffort%2CGNH_AppDev_Effort%2CGNH_AppSystem_Effort%2CCustom_LabellingEffort%2CCustom_MarketAccessEffort%2CCustom_RegulatoryCertificationEffort%2CCustom_RegulatoryProductComplianceEffort%2CGNH_FT_Effort%2CGNH_OS_Effort%2CGNH_Sound_Effort%2CGNH_System_Effort%2CGNH_Dev_Effort%2CGNH_QA_Effort%2CCustom_FeatureElectronicIntegration%2CCustom_FeatureRadioSystems%2CCustom_FeatureMaterialsandReliability%2CCustom_FeatureMechanicalDevelopmentCPH%2CCustom_FeatureMechanicalDevelopmentXMN%2CCustom_FeatureAcousticDevelopmentCPH%2CCustom_FeatureAcousticDevelopmentXMN%2CCustom_FeatureQualificationXMN", "level": 2},
    {"url": "https://analytics.dev.azure.com/GNHearing/_odata/v3.0/WorkItems?%24filter=Project%2FProjectName+eq+%27GNH%27+and+Parent%2FParent%2FParent%2FWorkItemId+ne+null+and+Parent%2FParent%2FParent%2FArea%2FAreaPath+eq+%27GNH%27+and+Parent%2FParent%2FParent%2FWorkItemType+eq+%27Portfolio+Epic%27+and+Parent%2FState+ne+%27Closed%27+and+Parent%2FState+ne+%27Removed%27+and+Parent%2FParent%2FState+ne+%27Closed%27+and+Parent%2FParent%2FState+ne+%27Removed%27+and+Parent%2FParent%2FParent%2FState+ne+%27Closed%27+and+Parent%2FParent%2FParent%2FState+ne+%27Removed%27+and+Parent%2FParent%2FParent%2FParent%2FWorkItemId+eq+null&%24select=WorkItemId%2CWorkItemType%2CTitle%2CState%2CCustom_ReasonForMustDo%2CParentWorkItemId%2CGNH_ValueType%2CCustom_RequestforPI%2CCustom_Prioritized%2CCustom_TestAutomationEffort%2CCustom_TestManuelEffort%2CCustom_TestSystemEffort%2CGNH_AppDev_Effort%2CGNH_AppSystem_Effort%2CCustom_LabellingEffort%2CCustom_MarketAccessEffort%2CCustom_RegulatoryCertificationEffort%2CCustom_RegulatoryProductComplianceEffort%2CGNH_FT_Effort%2CGNH_OS_Effort%2CGNH_Sound_Effort%2CGNH_System_Effort%2CGNH_Dev_Effort%2CGNH_QA_Effort%2CCustom_FeatureElectronicIntegration%2CCustom_FeatureRadioSystems%2CCustom_FeatureMaterialsandReliability%2CCustom_FeatureMechanicalDevelopmentCPH%2CCustom_FeatureMechanicalDevelopmentXMN%2CCustom_FeatureAcousticDevelopmentCPH%2CCustom_FeatureAcousticDevelopmentXMN%2CCustom_FeatureQualificationXMN", "level": 3},
]

def extract_function(ti, url, level):
    azure_devops_conn_id = 'devops'
    azure_devops_conn = BaseHook.get_connection(azure_devops_conn_id)
    user = azure_devops_conn.login
    password = azure_devops_conn.password

    response = requests.get(url, auth=HTTPBasicAuth(user, password))
    data = response.json()
    df = pd.json_normalize(data['value'])
    tbl_name = f"level_{level}"
    
    ti.xcom_push(key=f'{tbl_name}_dataframe', value=df)
    ti.xcom_push(key=f'{tbl_name}_tablename', value=tbl_name)
    
    return df, tbl_name

def load_to_db(ti, level):
    postgres_external_conn_id = 'Postgres_external'
    postgres_conn = BaseHook.get_connection(postgres_external_conn_id)
    user = postgres_conn.login
    password = postgres_conn.password
    database = postgres_conn.database
    # Retrieve the DataFrame from XCom
    tbl_name = ti.xcom_pull(key=f'level_{level}_tablename')
    df = ti.xcom_pull(key=f'level_{level}_dataframe')

    # Create a database connection
    engine = create_engine(f'postgresql://{user}:{password}@host.docker.internal:5432/{database}')

    # Write DataFrame to the SQL database
    df.to_sql(tbl_name, engine, if_exists='replace', index=False)

with DAG(dag_id='odata_to_postgres', default_args=default_args, schedule_interval='@daily') as dag:
    extract_tasks = []
    load_to_db_tasks = []

    for level_info in levels:
        extract = PythonOperator(
            task_id=f'extract_{level_info["level"]}',
            python_callable=extract_function,
            op_kwargs={'url': level_info["url"], 'level': level_info["level"]},
        )
        extract_tasks.append(extract)

        load_to_db_task = PythonOperator(
        task_id=f'load_to_db_{level_info["level"]}',
        python_callable=load_to_db,
        op_kwargs={'level': level_info["level"]},
    )
        load_to_db_tasks.append(load_to_db_task)

        # Set the dependency for the load task to follow the extract task
        extract >> load_to_db_task