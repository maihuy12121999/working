from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG
#
# @author: anhlt
#
from common.utils import base64_encode

with RvDAG(
        'gsheet_to_s3_emc_data',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Sync EMC Data to S3',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='1 0 * * *',
        start_date=datetime(2022, 11, 20),
        end_date=None,
        catchup=True,
        tags=['Data Sync', 'GSheet -> S3', 'EMC GSheet Data'],
) as dag:
    from spark_job_synchronizer_factory import TRANSACTION_REPORT

    version = 'v6.6'

    cost_operator = RLocalSparkOperator(
        task_id="emc_cost_data",
        project=TRANSACTION_REPORT,
        version=version,
        main_clazz='rever.etl.emc.GSheetCostDataRunner',
        arguments={
            "RV_JOB_ID": 'gsheet_emc_data',
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "RV_S3_ACCESS_KEY": Variable.get('RV_S3_ACCESS_KEY', ''),
            "RV_S3_SECRET_KEY": Variable.get('RV_S3_SECRET_KEY', ''),
            "RV_S3_REGION": Variable.get('RV_S3_REGION', ''),
            "RV_S3_BUCKET": Variable.get('RV_S3_BUCKET', ''),
            "RV_S3_PARENT_PATH": Variable.get('RV_S3_PARENT_PATH').strip(),
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            'gsheet_cost_service_account_key': base64_encode(Variable.get('RV_BI_GOOGLE_SERVICE_ACCOUNT')),
            'gsheet_cost_spreadsheet_id': Variable.get('FINANCE_COST_CLOSED_DEAL_SPREADSHEET_ID'),
            'gsheet_cost_sheet_name': 'PL_Data_Closed',
            'gsheet_cost_data_range': 'A1:Y1',
            'gsheet_cost_with_header': 'true',
            'merge_after_write': 'true',
            'forcerun.enabled': '{{ dag_run.conf["forcerun.enabled"] if dag_run.conf else "false" }}',
            'forcerun.from_date': '{{ dag_run.conf["forcerun.from_date"] if dag_run.conf else "" }}',
            'forcerun.to_date': '{{ dag_run.conf["forcerun.to_date"] if dag_run.conf else "" }}'
        }
    )

    closed_deal_operator = RLocalSparkOperator(
        task_id="closed_deal_data",
        project=TRANSACTION_REPORT,
        version=version,
        main_clazz='rever.etl.emc.GSheetDealDataRunner',
        arguments={
            "RV_JOB_ID": 'gsheet_emc_data',
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "RV_S3_ACCESS_KEY": Variable.get('RV_S3_ACCESS_KEY', ''),
            "RV_S3_SECRET_KEY": Variable.get('RV_S3_SECRET_KEY', ''),
            "RV_S3_REGION": Variable.get('RV_S3_REGION', ''),
            "RV_S3_BUCKET": Variable.get('RV_S3_BUCKET', ''),
            "RV_S3_PARENT_PATH": '',
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            'gsheet_deal_service_account_key': base64_encode(Variable.get('RV_BI_GOOGLE_SERVICE_ACCOUNT')),
            'gsheet_deal_spreadsheet_id': Variable.get('FINANCE_COST_CLOSED_DEAL_SPREADSHEET_ID'),
            'gsheet_deal_sheet_name': '"Deals - Closed"',
            'gsheet_deal_data_range': 'A1:AC1',
            'gsheet_deal_with_header': 'true',
            'merge_after_write': 'true'
        }
    )

    closed_deal_operator >> cost_operator
