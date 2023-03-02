from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'admin_clickhouse',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Admin Clickhouse',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': False,
            'retries': 0
        },
        schedule_interval='40 * * * *',
        start_date=datetime(2022, 12, 1),
        end_date=None,
        catchup=False,
        tags=['Admin', 'ClickHouse'],
) as dag:
    from spark_job_synchronizer_factory import DATA_SYNC_JOB_PROJECT

    version = 'v8.3'

    optimize_table_operator = RLocalSparkOperator(
        task_id="admin_optimize_clickhouse_tables",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.AdminCHOptimizeTableRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'analytics',
            'optimize_database_watchlist': Variable.get('OPTIMIZE_DATABASE_WATCHLIST', 'analytics,finance_analytics'),
            'max_size_in_bytes': Variable.get('OPTIMIZE_DATABASE_THRESHOLD', '229124506')
        }
    )

    optimize_table_operator
