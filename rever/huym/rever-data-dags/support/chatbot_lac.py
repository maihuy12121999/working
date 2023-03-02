from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
from common.utils import base64_encode

with RvDAG(
        'chatbot_lac',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='LAC Chatbot Export',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': False
        },
        schedule_interval='5 * * * *',
        start_date=datetime(2022, 6, 16),
        end_date=None,
        catchup=False,
        tags=['LAC Chatbot', 'Export'],
) as dag:
    from spark_job_synchronizer_factory import SUPPORT_REPORT

    version = 'v1.8'
    export_lac_request_to_google_sheet = RLocalSparkOperator(
        task_id="export_lac_request_to_google_sheet",
        project=SUPPORT_REPORT,
        version=version,
        main_clazz='rever.etl.support.LacChatbotEntryPoint',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "CH_DRIVER": Variable.get('RV_CH_RAP_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            "export_google_service_account_base64": base64_encode(Variable.get('RV_BI_GOOGLE_SERVICE_ACCOUNT')),
            "export_spreadsheet_id": Variable.get('RV_LAC_CHATBOT_SHEET_ID'),
            "export_sheet_name": Variable.get('RV_LAC_CHATBOT_SHEET'),
            "export_start_row_index": "1",
            "export_sheet_column_count": "17",
            "export_sheet_start_edit_column_index": "0",
            "export_sheet_end_edit_column_index": "16",
            'merge_after_write': 'true'
        }
    )

    export_lac_request_to_google_sheet
