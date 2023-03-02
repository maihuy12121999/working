from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG
#
# @author: anhlt
#
from common.utils import base64_encode

with RvDAG(
        'sync_gsheet_daily_huddle_kpi_to_clickhouse',
        owner='vylnt1',
        emails=['vylnt1@rever.vn'],
        description='Sync Daily Huddle KPI To Clickhouse',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': True
        },
        schedule_interval='0 4 * * *',
        start_date=datetime(2022, 11, 30),
        end_date=None,
        catchup=True,
        tags=['Data Sync', 'Daily Huddle KPI', 'Google Sheet -> ClickHouse'],
) as dag:
    from spark_job_synchronizer_factory import DATA_SYNC_JOB_PROJECT

    version = 'v8.3'

    daily_huddle_kpi_operator = RLocalSparkOperator(
        task_id="sync_gsheet_daily_huddle_kpi_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.DailyHuddleKPIToCHRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            'target_table': 'rever_daily_huddle_kpi',
            'gsheet_huddle_kpi_service_account_key': base64_encode(Variable.get('RV_BI_GOOGLE_SERVICE_ACCOUNT')),
            'gsheet_huddle_kpi_spreadsheet_id': '1xUhHLZ8Eo5NV7_xsXbrL430J5Ob_VHKmZ95F2656NGo',
            'gsheet_huddle_kpi_sheet_name': 'Daily',
            'gsheet_huddle_kpi_data_range': 'A1:L1',
            'gsheet_huddle_kpi_with_header': 'true',
            'merge_after_write': 'true'
        }

    )

    daily_huddle_kpi_operator
