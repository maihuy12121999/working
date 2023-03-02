from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG
#
# @author: anhlt
#
from common.utils import base64_encode

with RvDAG(
        'sync_gsheet_finance_data_to_clickhouse',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Sync Finance Data To Clickhouse',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': True
        },
        schedule_interval='20 0 * * *',
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['Data Sync', 'Finance Data', 'Google Sheet -> ClickHouse'],
) as dag:
    from spark_job_synchronizer_factory import DATA_SYNC_JOB_PROJECT

    version = 'v8.3'

    data_sync_job_operator = RLocalSparkOperator(
        task_id="sync_gsheet_finance_data_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.FinanceDataRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'finance_analytics',
            "google_service_account_encoded": base64_encode(Variable.get('RV_BI_GOOGLE_SERVICE_ACCOUNT')),
            "finance_data_sheet_id": Variable.get('RV_FINANCE_DATA_SHEET_ID'),
            'merge_after_write': 'true'
        }
    )

    data_sync_job_operator
