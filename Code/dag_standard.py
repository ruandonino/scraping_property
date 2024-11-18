import os

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator
)
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

PROJECT_ID = os.environ.get('GCP_PROJECT')
REGION = 'us-east4'
CLUSTER_NAME = 'ephemeral-spark-cluster'
API_EXTRACT_URI = 'gs://python_files_stock/git_repository/pyspark_extract.py'
#JOIN_FILES_URI = 'gs://python_files_stock/git_repository/join_by_data.py'
PROCESS_URI = 'gs://python_files_stock/git_repository/process_data_spark.py'

API_EXTRACT_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": API_EXTRACT_URI,
    "jar_file_uris":["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
    }
}
'''
JOIN_FILES_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": JOIN_FILES_URI,
    "jar_file_uris":["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
    }
}
'''
PROCESS_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PROCESS_URI,
                    "jar_file_uris":["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
    }
}

args = {
    'owner': 'packt-developer',
}
CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    master_machine_type="n1-standard-2",
    master_disk_size=50,
    worker_machine_type="e2-medium",
    worker_disk_size=30,
    num_workers=2,
    num_masters=1,
    init_actions_uris=[f"gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh"],
    enable_component_gateway=True,
    optional_components = ['JUPYTER'],
    metadata={"PIP_PACKAGES": "GoogleNews==1.6.14"}
    ).make()

with DAG(
        dag_id='dataproc_ephemeral_cluster_job',
    schedule_interval='0 5 * * *',
    start_date=days_ago(1),
    default_args=args
) as dag:
    checkout_repo = BashOperator(
        task_id='checkout_repo',
        bash_command='rm -rf /home/valdairdonino/github_repo && git clone https://github.com/valdairdonino/stock_news_pipeline.git /home/valdairdonino/github_repo',
    )

    copy_to_gcs = LocalFilesystemToGCSOperator(
        task_id='copy_to_gcs',
        src=['/home/valdairdonino/github_repo/Code_ETL/pyspark_extract.py', '/home/valdairdonino/github_repo/Code_ETL/process_data_spark.py'],
        dst='git_repository/',
        bucket='python_files_stock',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_GENERATOR_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )
    
    api_extract_task = DataprocSubmitJobOperator(
        task_id="api_extract_task", job=API_EXTRACT_JOB, location=REGION, project_id=PROJECT_ID
    )
    process_task = DataprocSubmitJobOperator(
        task_id="process_task", job=PROCESS_JOB, location=REGION, project_id=PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
    )
checkout_repo  >>  copy_to_gcs >> create_cluster >> api_extract_task >> process_task >> delete_cluster
