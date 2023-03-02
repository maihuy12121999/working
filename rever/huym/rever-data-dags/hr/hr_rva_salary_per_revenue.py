from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#

with RvDAG(
        'hr_rva_salary_per_revenue',
        owner='vylnt1',
        emails=['vylnt1@rever.vn', 'anhlt@rever.vn'],
        description='HR - RVA Salary Per Revenue Rate',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='15 3 * * *',
        # start_date=datetime(2022, 1, 1),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['HR', 'RVA Salary/Revenue Rate'],
) as dag:
    from spark_job_synchronizer_factory import HR_REPORT

    version = 'v1.9'

    # Wait for emc/closed_transactions.py to complete first.
    emc_closed_transactions_dag_sensor = ExternalTaskSensor(
        task_id='wait_for_emc_closed_transactions',
        external_dag_id="closed_transactions",
        execution_delta=timedelta(minutes=10),
        check_existence=True
    )

    rva_salary_per_revenue_operator = RLocalSparkOperator(
        task_id="exec_hr_rva_salary_revenue",
        project=HR_REPORT,
        version=version,
        main_clazz='rever.etl.hr.RvaSalaryPerRevenueRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "CH_DRIVER": Variable.get('RV_CH_RAP_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "RV_S3_ACCESS_KEY": Variable.get('RV_S3_ACCESS_KEY', ''),
            "RV_S3_SECRET_KEY": Variable.get('RV_S3_SECRET_KEY', ''),
            "RV_S3_REGION": Variable.get('RV_S3_REGION', ''),
            "RV_S3_BUCKET": Variable.get('RV_S3_BUCKET', ''),
            "RV_S3_PARENT_PATH": Variable.get('RV_S3_PARENT_PATH').strip(),
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "closed_transaction_table": 'analytics.closed_transaction_1',
            'rva_salary_table': 'airbyte._airbyte_raw_hr_rva_salary_cost_thu_nhap',
            "rva_salary_per_revenue_rate_topic": 'rap.analytics.rva_salary_per_revenue_rate',
            "merge_after_write": 'true'
        }
    )

    emc_closed_transactions_dag_sensor >> rva_salary_per_revenue_operator
