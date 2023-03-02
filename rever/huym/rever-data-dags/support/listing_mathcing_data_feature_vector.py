from datetime import datetime
from common.rv_dag import RvDAG
from common.rspark_local_operator import RLocalSparkOperator
from airflow.models import Variable
from common.utils import base64_encode

with RvDAG(
        "listing_matching_property_feature_vector",
        owner='huym',
        emails=['huym@rever.vn'],
        description='feature vector data for listing matching',
        default_args={
            'wait_for_downstream': False
        },
        schedule_interval=None,
        start_date=datetime(2022, 12, 1),
        end_date=None,
        catchup=True,
        tags=['AI', 'Listing Matching', 'Feature Vector']
) as dag:
    from spark_job_synchronizer_factory import SUPPORT_REPORT

    version = 'v2.3'
    listing_matching_feature_vector_operator = RLocalSparkOperator(
        task_id='export_listing_matching_feature_vector',
        project=SUPPORT_REPORT,
        version=version,
        main_clazz='rever.etl.support.ListingMatchingDataFeatureVectorRunner',
        arguments={
            "RV_JOB_ID": dag.dag_id,
            "RV_EXECUTION_DATE": '{{ execution_date }}',
            "CH_DRIVER": Variable.get('RV_CH_RAP_DRIVER'),
            "CH_HOST": Variable.get('RV_CH_RAP_HOST'),
            "CH_PORT": Variable.get('RV_CH_RAP_PORT'),
            "CH_USER_NAME": Variable.get('RV_CH_RAP_USERNAME'),
            "CH_PASSWORD": Variable.get('RV_CH_RAP_PASSWORD'),
            "listing_matching_vector_config": base64_encode(Variable.get('LISTING_MATCHING_VECTOR_CONFIG'))
        }
    )

    listing_matching_feature_vector_operator
