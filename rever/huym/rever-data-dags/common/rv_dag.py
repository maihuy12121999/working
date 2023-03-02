from datetime import timedelta

from airflow import DAG
#
# @author: anhlt
#
from airflow.models import TaskInstance, DagRun
from pytz import timezone

from common.debug_service_notification import DebugServiceNotification
from common.utils import is_test_mode

doc_md = """
# Default config

### ForceRun

There are 3 params in order to force run a DAG.

- `forcerun.enabled`: True if we want to force run this DAG.
- `forcerun.from_date` & `forcerun.to_date`: (yyyy-MM-dd) the date range to force run this DAG, inclusive

E.g: The below setting is used to force run a DAG from 01 Jan 2022 to 05 Jan 2022

```json
{
  "forcerun.enabled": "true",
  "forcerun.from_date": "2022-01-01",
  "forcerun.to_date": "2022-01-05"
}
```

### Data Sync All

For DAG tagged with `Data Sync` only. If we want to sync all data:

```json
{
  "is_sync_all": "true"
}
```
### Put Kafka

```json
{
  "is_put_kafka": "true"
}
```
"""


class RvDAG(DAG):
    def __init__(self, dag_id: str, owner: str, emails: list, **kwargs):
        if kwargs['default_args'] is not None:
            kwargs['default_args'].update({
                'owner': owner,
                'email': emails,
                'on_failure_callback': rv_on_failure_callback,
                'email_on_failure': False,
                'email_on_retry': False,

            })
        else:
            kwargs['default_args'] = {
                'owner': owner,
                'email': emails,
                'on_failure_callback': rv_on_failure_callback,
                'email_on_failure': False,
                'email_on_retry': False,
            }
        if 'depends_on_past' not in kwargs['default_args']:
            kwargs['default_args']['depends_on_past'] = True
        if 'retries' not in kwargs['default_args']:
            kwargs['default_args']['retries'] = 3
        if 'retry_delay' not in kwargs['default_args']:
            kwargs['default_args']['retry_delay'] = timedelta(minutes=2)
            kwargs['default_args']['max_retry_delay'] = timedelta(minutes=10)
            kwargs['default_args']['retries'] = 3
        kwargs['default_args']['retry_exponential_backoff'] = True

        if 'default_view' not in kwargs['default_args']:
            kwargs['default_args']['default_view'] = 'tree'

        kwargs['catchup'] = False if is_test_mode() else kwargs.get('catchup', False)
        kwargs['doc_md'] = doc_md


        super().__init__(dag_id, **kwargs)


def rv_on_failure_callback(context):
    dag: DAG = context['dag']
    dag_run: DagRun = context['dag_run']
    task: TaskInstance = context['task_instance']

    owner = dag.default_args['owner']
    emails = ",".join(dag.default_args['email'])

    DebugServiceNotification.notify(
        f"DAG Failure",
        {
            'Owner': f"{owner} - {emails}",
            'Dag': task.dag_id,
            'Task': task.task_id,
            'Execution date': dag_run.execution_date.astimezone(timezone('Asia/Ho_Chi_Minh')),
        },
        f"Task failed for dag run: {dag_run.run_id}\nWith config: {dag_run.conf}\n{context['exception']}"
    )
