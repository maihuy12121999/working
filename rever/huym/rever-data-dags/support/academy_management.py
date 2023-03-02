from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#

with RvDAG(
        'support_academy_management',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Academy Management',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': False
        },
        schedule_interval='0 2 * * *',
        # start_date=datetime(2022, 6, 1),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=False,
        tags=['Academy Management', 'Support'],
) as dag:
    from spark_job_synchronizer_factory import SUPPORT_REPORT

    version = 'v1.9'

    spark_job_operator = RLocalSparkOperator(
        task_id="exec_rever_academy_support",
        project=SUPPORT_REPORT,
        version=version,
        main_clazz='rever.etl.support.ReverAcademyEntryPoint',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "CH_DRIVER": Variable.get('RV_CH_RAP_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            "export_google_service_account_base64": Variable.get('RV_BI_GOOGLE_SERVICE_ACCOUNT'),
            "ignore_staff_quit_before_date": "2021-01-01",
            "export_spreadsheet_id": Variable.get('RV_ACADEMY_MANAGEMENT_SHEET_ID'),
            "export_sheet_name": Variable.get('RV_ACADEMY_MANAGEMENT_STAFF_SHEET'),
            "export_start_row_index": "2",
            "export_sheet_column_count": "19",
            "export_sheet_start_edit_column_index": "0",
            "export_sheet_end_edit_column_index": "15",
            "rever_academy_student_topic": "rap.analytics.rever_academy_students",
            'merge_after_write': 'true'
        }
    )

    spark_job_operator
