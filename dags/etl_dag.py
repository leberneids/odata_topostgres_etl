from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from sqlalchemy import create_engine
import logging
from pydantic import BaseModel

default_args = {
    'owner': 'lluis',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

# List of dictionaries containing task information
levels = [
    {"url": "https://analytics.dev.azure.com/GNHearing/_odata/v3.0/WorkItems?%24filter=%28Project%2FProjectName+eq+%27GNH%27+and+WorkItemType+eq+%27Portfolio+Epic%27+and+Area%2FAreaPath+eq+%27GNH%27+and+State+ne+%27Closed%27+and+State+ne+%27Removed%27%29&%24select=WorkItemId%2CWorkItemType%2CTitle%2CState%2CTagNames", "level": 0},
    {"url": "https://analytics.dev.azure.com/GNHearing/_odata/v3.0/WorkItems?%24filter=+Project%2FProjectName+eq+%27GNH%27+and+Parent%2FWorkItemId+ne+null+and+Parent%2FArea%2FAreaPath+eq+%27GNH%27+and+Parent%2FWorkItemType+eq+%27Portfolio+Epic%27+and+Parent%2FState+ne+%27Closed%27+and+Parent%2FState+ne+%27Removed%27&%24select=WorkItemId%2CWorkItemType%2CTitle%2CState%2CCustom_ReasonForMustDo%2CParentWorkItemId%2CGNH_ValueType%2CCustom_RequestforPI%2CCustom_Prioritized%2CCustom_TestAutomationEffort%2CCustom_TestManuelEffort%2CCustom_TestSystemEffort%2CGNH_AppDev_Effort%2CGNH_AppSystem_Effort%2CCustom_LabellingEffort%2CCustom_MarketAccessEffort%2CCustom_RegulatoryCertificationEffort%2CCustom_RegulatoryProductComplianceEffort%2CGNH_FT_Effort%2CGNH_OS_Effort%2CGNH_Sound_Effort%2CGNH_System_Effort%2CGNH_Dev_Effort%2CGNH_QA_Effort%2CCustom_FeatureElectronicIntegration%2CCustom_FeatureRadioSystems%2CCustom_FeatureMaterialsandReliability%2CCustom_FeatureMechanicalDevelopmentCPH%2CCustom_FeatureMechanicalDevelopmentXMN%2CCustom_FeatureAcousticDevelopmentCPH%2CCustom_FeatureAcousticDevelopmentXMN%2CCustom_FeatureQualificationXMN", "level": 1},
    {"url": "https://analytics.dev.azure.com/GNHearing/_odata/v3.0/WorkItems?%24filter=Project%2FProjectName+eq+%27GNH%27+and+Parent%2FWorkItemId+ne+null+and+Parent%2FParent%2FProject%2FProjectName+eq+%27GNH%27+and+Parent%2FParent%2FArea%2FAreaPath+eq+%27GNH%27+and+Parent%2FParent%2FWorkItemType+eq+%27Portfolio+Epic%27+and+Parent%2FParent%2FState+ne+%27Closed%27+and+State+ne+%27Removed%27&%24select=WorkItemId%2CWorkItemType%2CTitle%2CState%2CParentWorkItemId%2C+Custom_ReasonForMustDo%2CGNH_ValueType%2CCustom_RequestforPI%2CCustom_Prioritized%2CCustom_TestAutomationEffort%2CCustom_TestManuelEffort%2CCustom_TestSystemEffort%2CGNH_AppDev_Effort%2CGNH_AppSystem_Effort%2CCustom_LabellingEffort%2CCustom_MarketAccessEffort%2CCustom_RegulatoryCertificationEffort%2CCustom_RegulatoryProductComplianceEffort%2CGNH_FT_Effort%2CGNH_OS_Effort%2CGNH_Sound_Effort%2CGNH_System_Effort%2CGNH_Dev_Effort%2CGNH_QA_Effort%2CCustom_FeatureElectronicIntegration%2CCustom_FeatureRadioSystems%2CCustom_FeatureMaterialsandReliability%2CCustom_FeatureMechanicalDevelopmentCPH%2CCustom_FeatureMechanicalDevelopmentXMN%2CCustom_FeatureAcousticDevelopmentCPH%2CCustom_FeatureAcousticDevelopmentXMN%2CCustom_FeatureQualificationXMN", "level": 2},
    {"url": "https://analytics.dev.azure.com/GNHearing/_odata/v3.0/WorkItems?%24filter=Project%2FProjectName+eq+%27GNH%27+and+Parent%2FParent%2FParent%2FWorkItemId+ne+null+and+Parent%2FParent%2FParent%2FArea%2FAreaPath+eq+%27GNH%27+and+Parent%2FParent%2FParent%2FWorkItemType+eq+%27Portfolio+Epic%27+and+Parent%2FState+ne+%27Closed%27+and+Parent%2FState+ne+%27Removed%27+and+Parent%2FParent%2FState+ne+%27Closed%27+and+Parent%2FParent%2FState+ne+%27Removed%27+and+Parent%2FParent%2FParent%2FState+ne+%27Closed%27+and+Parent%2FParent%2FParent%2FState+ne+%27Removed%27+and+Parent%2FParent%2FParent%2FParent%2FWorkItemId+eq+null&%24select=WorkItemId%2CWorkItemType%2CTitle%2CState%2CCustom_ReasonForMustDo%2CParentWorkItemId%2CGNH_ValueType%2CCustom_RequestforPI%2CCustom_Prioritized%2CCustom_TestAutomationEffort%2CCustom_TestManuelEffort%2CCustom_TestSystemEffort%2CGNH_AppDev_Effort%2CGNH_AppSystem_Effort%2CCustom_LabellingEffort%2CCustom_MarketAccessEffort%2CCustom_RegulatoryCertificationEffort%2CCustom_RegulatoryProductComplianceEffort%2CGNH_FT_Effort%2CGNH_OS_Effort%2CGNH_Sound_Effort%2CGNH_System_Effort%2CGNH_Dev_Effort%2CGNH_QA_Effort%2CCustom_FeatureElectronicIntegration%2CCustom_FeatureRadioSystems%2CCustom_FeatureMaterialsandReliability%2CCustom_FeatureMechanicalDevelopmentCPH%2CCustom_FeatureMechanicalDevelopmentXMN%2CCustom_FeatureAcousticDevelopmentCPH%2CCustom_FeatureAcousticDevelopmentXMN%2CCustom_FeatureQualificationXMN", "level": 3},
]

name_change = {"Custom_FeatureAcousticDevelopmentCPH": "HW Acoustic Development CPH", 
             "Custom_FeatureAcousticDevelopmentXMN": "HW Acoustic Development XMN", 
             "Custom_FeatureElectronicIntegration": "HW Electronic Integration", 
             "Custom_FeatureMaterialsandReliability": "HW Materials and Reliability",
             "Custom_FeatureMechanicalDevelopmentCPH": "HW Mechanical Development CPH",
             "Custom_FeatureMechanicalDevelopmentXMN": "HW Mechanical Development XMN",
             "Custom_FeatureQualificationXMN": "HW Qualification XMN",
             "Custom_FeatureRadioSystems": "HW Radio Systems",
             "Custom_LabellingEffort": "CQ Labelling",
             "Custom_MarketAccessEffort": "CQ Market Access",
             "Custom_RegulatoryCertificationEffort": "CQ Regulatory Certification",
             "Custom_RegulatoryProductComplianceEffort": "CQ Regulatory Product Compliance",
             "Custom_TestAutomationEffort": "APP Test Automation",
             "Custom_TestManuelEffort": "APP Test Manuel",
             "Custom_TestSystemEffort": "APP Test System",
             "GNH_AppDev_Effort": "APP Development",
             "GNH_AppSystem_Effort": "APP System",
             "GNH_Dev_Effort": "HCP Development",
             "GNH_FT_Effort": "DSW FT",
             "GNH_OS_Effort": "DSW OS",
             "GNH_QA_Effort": "HCP Test",
             "GNH_Sound_Effort": "DSW Sound",
             "GNH_System_Effort": "DSW System",
             "Custom_LifeCycleManagementEffort": "CQ Life Cycle Management"}


@task
def extract_data(url: str, level: int):
    try:
        azure_devops_conn_id = 'azure_devops'
        azure_devops_conn = BaseHook.get_connection(azure_devops_conn_id)
        user = azure_devops_conn.login
        password = azure_devops_conn.password

        response = requests.get(url, auth=HTTPBasicAuth(user, password))
        data = response.json()
        df = pd.json_normalize(data['value'])
        df = df.rename(columns=name_change)
        tbl_name = f"level_{level}"
    
        logging.info(f"Data extracted successfully for level {level}")
        return df.to_json(orient='split'), tbl_name
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")  # HTTP error
    except Exception as err:
        logging.error(f"An error occurred: {err}")  # Other errors


@task
def load_to_db(data):
    try:
        df_json, tbl_name = data  # Now you unpack it inside the task
        postgres_external_conn_id = 'postgres_external'
        postgres_conn = BaseHook.get_connection(postgres_external_conn_id)
        user = postgres_conn.login
        password = postgres_conn.password
        database = 'Azuredevops'
        
        engine = create_engine(f'postgresql://{user}:{password}@host.docker.internal:5432/{database}')
        df = pd.read_json(df_json, orient='split')
        
        # Write DataFrame to the SQL database
        df.to_sql(tbl_name, engine, if_exists='replace', index=False)

        logging.info(f"Data loaded successfully into {tbl_name}")
    except Exception as err:
        logging.error(f"Failed to load data into database: {err}")

with DAG('odata_to_postgres', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    for level_info in levels:
        # Create the extract_data task
        extract_output = extract_data(url=level_info["url"], level=level_info["level"])
        # Directly pass the extract_output (XComArg) to load_to_db
        load_to_db(extract_output)