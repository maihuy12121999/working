from datetime import datetime

from airflow.models import Variable

from common.rspark_local_operator import RLocalSparkOperator
from common.rv_dag import RvDAG

#
# @author: anhlt
#
with RvDAG(
        'mysql_to_clickhouse_rever_personal_contact',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='Sync Personal Contact To Clickhouse',
        default_args={
            'wait_for_downstream': False,
            'depends_on_past': True
        },
        schedule_interval='*/8 * * * *',
        start_date=datetime(2022, 12, 5),
        end_date=None,
        catchup=True,
        tags=['Data Sync', 'MySQL -> ClickHouse', 'Personal Contact'],
) as dag:
    from spark_job_synchronizer_factory import DATA_SYNC_JOB_PROJECT

    version = 'v8.3'

    sync_personal_contact_operator = RLocalSparkOperator(
        task_id="sync_personal_contact_to_clickhouse",
        project=DATA_SYNC_JOB_PROJECT,
        version=version,
        main_clazz='rever.etl.data_sync.PContactToCHRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "MYSQL_DRIVER": Variable.get('MYSQL_DRIVER'),
            "MYSQL_HOST": Variable.get('MYSQL_HOST'),
            "MYSQL_PORT": Variable.get('MYSQL_PORT'),
            "MYSQL_USER_NAME": Variable.get('MYSQL_USER_NAME'),
            "MYSQL_PASSWORD": Variable.get('MYSQL_PASSWORD'),
            "MYSQL_DB": 'rever_contact',
            "CH_DRIVER": Variable.get('CH_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "CH_DB": 'datamart',
            "RV_PUSH_KAFKA_HOST": Variable.get('RV_PUSH_KAFKA_HOST'),
            "RV_PUSH_KAFKA_SK": Variable.get('RV_PUSH_KAFKA_SK'),
            "source_table": 'personal_contact',
            "target_table": 'contact_personal_contact',
            "target_batch_size": '1000',
            'merge_after_write': 'true',
            'is_put_kafka': '{{ dag_run.conf.get("is_put_kafka", "false") if dag_run.conf else "false" }}',
            "data_delivery_enrichment_p_contact_topic": "data_delivery.enrichment.personal_contact",
            'is_sync_all': '{{ dag_run.conf.get("is_sync_all", "false") if dag_run.conf else "false" }}',
        }
    )

    sync_personal_contact_operator
