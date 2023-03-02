class PathBuilder:

    def __new__(cls, *args, **kwargs):
        raise TypeError('Static classes cannot be instantiated')

    @classmethod
    def remote_job_path(cls, job: str, version: str, is_test_mode: bool = False):
        version = 'latest' if is_test_mode else version
        return f'spark_jobs/{job}/{version}'

    @classmethod
    def job_path(cls, job: str, version: str, is_test_mode: bool = False):
        version = 'latest' if is_test_mode else version
        return f'/opt/airflow/dags/.cache/{job}/{version}'
