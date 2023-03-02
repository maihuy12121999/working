from datetime import datetime

#
# @author: anhlt
#
from airflow.providers.postgres.operators.postgres import PostgresOperator

from common.rv_dag import RvDAG

with RvDAG(
        'redash_cleanup_expire_result',
        owner='anhlt',
        emails=['anhlt@rever.vn'],
        description='A tool to cleanup expired query result from Redash',
        default_args={
            'depends_on_past': False,
            'wait_for_downstream': False
        },
        schedule_interval='0 5 * * *',
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['Cleanup Tool', 'Redash'],
) as dag:
    cleanup_query_result_postpres = PostgresOperator(
        task_id="cleanup_query_result_postpres",
        postgres_conn_id="rever_postpres_for_redash",
        sql="""
            update queries set latest_query_data_id =null 
            where 1=1
            and ('DROPDOWNLIST'=ANY(tags)
            or 'FILTER'=ANY(tags)
            or 'PARAMETERS'=ANY(tags)
            )=false 
            and latest_query_data_id in (	select id from query_results
                where 1=1
                and retrieved_at <= (current_date - interval '7' day)
            );
            delete from query_results 
            where id in (
                select latest_query_data_id
                from queries
                where 1=1
                and ('DROPDOWNLIST'=ANY(tags)
                or 'FILTER'=ANY(tags)
                or 'PARAMETERS'=ANY(tags)
                )=false 
            )
            and retrieved_at <= (current_date - interval '7' day);
          """,
        autocommit=True

    )

    cleanup_event_postpres = PostgresOperator(
        task_id="cleanup_events_postpres",
        postgres_conn_id="rever_postpres_for_redash",
        sql="""
            delete from events 
            where created_at <= (current_date - interval '90' day);
          """,
        autocommit=True
    )

    vacuum_query_results = PostgresOperator(
        task_id="vacuum_query_result",
        postgres_conn_id="rever_postpres_for_redash",
        sql="""
            vacuum full query_results;
          """,
        autocommit=True

    )

    vacuum_events = PostgresOperator(
        task_id="vacuum_events",
        postgres_conn_id="rever_postpres_for_redash",
        sql="""
            vacuum full events;
          """,
        autocommit=True

    )

    [cleanup_query_result_postpres, cleanup_event_postpres] >> vacuum_query_results >> vacuum_events
