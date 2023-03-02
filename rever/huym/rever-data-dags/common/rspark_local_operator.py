from typing import Dict, Optional

from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.context import Context

from common.path_builder import PathBuilder
from common.utils import is_modified_ago, is_test_mode, deploy_mode


class RLocalSparkOperator(BashOperator):

    def __init__(self,
                 task_id: str,
                 project: str,
                 version: str,
                 main_clazz: str,
                 spark_config_dict: Optional[Dict[str, str]] = None,
                 arguments: Optional[Dict[str, str]] = None,
                 force_download: Optional[bool] = False,
                 **kwargs):

        self.project = project
        self.version = version
        self.force_download = force_download

        if arguments is not None:
            arguments = {
                "RV_NEXT_EXECUTION_DATE": '{{ next_execution_date }}',
                "RV_PREV_EXECUTION_DATE": '{{ prev_execution_date }}',
                "RV_PREV_SUCCESS_EXECUTION_DATE": '{{ prev_execution_date_success }}',
                "RV_MODE": deploy_mode(),
                'is_sync_all': '{{ dag_run.conf.get("is_sync_all", "false") if dag_run.conf else "false" }}',
                'forcerun.enabled': '{{ dag_run.conf.get("forcerun.enabled", "false") if dag_run.conf else "false" }}',
                'forcerun.from_date': '{{ dag_run.conf.get("forcerun.from_date", "") if dag_run.conf else "" }}',
                'forcerun.to_date': '{{ dag_run.conf.get("forcerun.to_date", "") if dag_run.conf else "" }}',
                **arguments
            }

        self.is_test_mode: bool = is_test_mode()

        super().__init__(
            task_id=task_id,
            bash_command=f"java -jar  {self.jar_runner(project, version, self.is_test_mode, main_clazz, spark_config_dict, arguments)}",
            **kwargs
        )

    def execute(self, context: Context):
        job_path = PathBuilder.job_path(self.project, self.version, self.is_test_mode)
        self._download_s3_to_local(
            PathBuilder.remote_job_path(self.project, self.version, self.is_test_mode),
            job_path,
            True if (self.force_download or (self.is_test_mode and is_modified_ago(job_path, 300 * 1000))) else False
        )
        super().execute(context)

    @classmethod
    def jar_runner(cls, project: str, version: str, is_test_mode: bool,
                   main_clazz: str,
                   spark_config_dict: Optional[Dict[str, str]] = None,
                   argument_dict: Optional[Dict[str, str]] = None):
        args = []

        if spark_config_dict is not None:
            for k, v in spark_config_dict.items():
                args.append(f'--conf')
                args.append(f"{k}={v}")
        else:
            config = {
                'spark.sql.shuffle.partitions': 8,
                'spark.sql.files.minPartitionNum': '1',
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.advisoryPartitionSizeInBytes': '64m',
                'spark.sql.adaptive.coalescePartitions.initialPartitionNum': 128,
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.adaptive.skewJoin.enabled': 'true',
                'spark.sql.adaptive.skewJoin.skewedPartitionFactor': '5',
                'spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes': '128M',
                'spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version': '2',
                'spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored': 'true',
                'spark.driver.memory': '4g',
                'spark.sql.legacy.allowUntypedScalaUDF': 'true'
            }
            for k, v in config.items():
                args.append(f'--conf')
                args.append(f"{k}={v}")

        if argument_dict is not None:
            for k, v in argument_dict.items():
                args.append(f'---{k}')
                args.append(v)

        return f'{PathBuilder.job_path(project, version, is_test_mode)}/{project}.jar {main_clazz} {" ".join(args)}'

    @classmethod
    def _download_s3_to_local(cls, s3_folder: str, local_folder: str, force_download: bool) -> bool:
        import os
        from common.utils import download_s3_folder, delete_file_or_folder
        try:
            if not os.path.exists(local_folder) or force_download:
                print(f"Preparing to download from: {s3_folder} with force_download={force_download}")
                download_s3_folder(
                    s3_folder,
                    local_folder,
                    aws_access_key_id=Variable.get('RV_S3_ACCESS_KEY'),
                    aws_secret_access_key=Variable.get('RV_S3_SECRET_KEY'),
                    aws_s3_bucket=Variable.get('RV_S3_BUCKET', ''),
                    region_name=Variable.get('RV_S3_REGION', ''),
                )
            else:
                print(f"{s3_folder} is downloaded.")
            return True
        except Exception as e:
            delete_file_or_folder(local_folder)
            raise e
