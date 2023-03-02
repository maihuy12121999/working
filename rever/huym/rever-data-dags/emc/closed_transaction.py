from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'closed_transactions',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Closed Transaction, Revenue, Cost and PL',
        default_args={
            'wait_for_downstream': False,
        },
        schedule_interval='5 3 * * *',
        # start_date=datetime(2022, 1, 1),
        start_date=datetime(2022, 11, 25),
        end_date=None,
        catchup=True,
        tags=['EMC', 'Closed Transaction & Revenue', 'Cost', "PL"],
) as dag:
    from spark_job_synchronizer_factory import TRANSACTION_REPORT

    version = 'v6.6'

    # Wait for historical/user_historical.py to complete first.
    user_historical_sensor = ExternalTaskSensor(
        task_id='wait_for_user_historical',
        external_dag_id="user_historical",
        execution_delta=timedelta(hours=3),
        check_existence=True
    )

    closed_transaction_and_revenue_operator = RLocalSparkOperator(
        task_id="closed_transaction",
        project=TRANSACTION_REPORT,
        version=version,
        main_clazz='rever.etl.emc.ClosedTransactionRunner',
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
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            "f2_agent_ids": Variable.get('F2_AGENT_IDS'),
            "ctv_agent_ids": Variable.get('CTV_AGENT_IDS'),
            "f2_team_ids": Variable.get('F2_TEAM_IDS'),
            "ctv_team_ids": Variable.get('CTV_TEAM_IDS'),
            "closed_transaction_topic": 'rap.analytics.closed_transaction',
            "oppo_historical_job_id": 'oppo_historical',
            "gsheet_emc_data_job_id": 'gsheet_emc_data',
            'merge_after_write': 'true'
        }
    )

    cost_operator = RLocalSparkOperator(
        task_id="cost",
        project=TRANSACTION_REPORT,
        version=version,
        main_clazz='rever.etl.emc.CostRunner',
        arguments={
            "RV_JOB_ID": 'cost',
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
            "RV_S3_PARENT_PATH": '',
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            "cost_topic": 'rap.analytics.cost',
            "back_office_cost_topic": 'rap.analytics.back_office_cost',
            "salary_topic": 'rap.analytics.salary_cost',
            "oppo_historical_job_id": 'oppo_historical',
            "gsheet_emc_data_job_id": 'gsheet_emc_data',
            'merge_after_write': 'true',
            'forcerun.enabled': '{{ dag_run.conf["forcerun.enabled"] if dag_run.conf else "false" }}',
            'forcerun.from_date': '{{ dag_run.conf["forcerun.from_date"] if dag_run.conf else "" }}',
            'forcerun.to_date': '{{ dag_run.conf["forcerun.to_date"] if dag_run.conf else "" }}'
        }
    )

    profit_and_loss_operator = RLocalSparkOperator(
        task_id="profit_and_loss",
        project=TRANSACTION_REPORT,
        version=version,
        main_clazz='rever.etl.emc.ProfitAndLossRunner',
        arguments={
            "RV_JOB_ID": "profit_and_loss",
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
            "RV_S3_PARENT_PATH": '',
            "RV_RAP_INGESTION_HOST": Variable.get('RV_RAP_INGESTION_HOST'),
            "RV_DATA_MAPPING_HOST": Variable.get('RV_DATA_MAPPING_HOST'),
            "RV_DATA_MAPPING_USER": Variable.get('RV_DATA_MAPPING_USER'),
            "RV_DATA_MAPPING_PASSWORD": Variable.get('RV_DATA_MAPPING_PASSWORD'),
            "profit_loss_topic": 'rap.analytics.profit_and_loss',
            "salary_cost_table": 'analytics.salary_cost_1',
            "cost_table": 'analytics.cost_1',
            "closed_transaction_table": 'analytics.closed_transaction_1',
            "back_office_cost_table": 'analytics.back_office_cost_1',
            "oppo_historical_job_id": 'oppo_historical',
            'merge_after_write': 'true',
            'forcerun.enabled': '{{ dag_run.conf["forcerun.enabled"] if dag_run.conf else "false" }}',
            'forcerun.from_date': '{{ dag_run.conf["forcerun.from_date"] if dag_run.conf else "" }}',
            'forcerun.to_date': '{{ dag_run.conf["forcerun.to_date"] if dag_run.conf else "" }}'
        }
    )

    user_historical_sensor >> [closed_transaction_and_revenue_operator, cost_operator] >> profit_and_loss_operator
