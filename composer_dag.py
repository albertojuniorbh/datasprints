from airflow import DAG
from airflow.utils.dates import days_ago


from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)

default_arguments = {"owner": "Alberto Dorea", 
                     "start_date": days_ago(1)}

with DAG(
    "Processing_NYCTaxi",
    schedule_interval="0 20 * * *",
    catchup=False,
    default_args=default_arguments,
) as dag:
    create_cluster = DataprocClusterCreateOperator(
        task_id='create_cluster',
        project_id='data-sprints-322619',
        cluster_name='spark-cluster-{{ds_nodash}}',
        num_workers=2,
        worker_machine_type='n1-standard-2',
        storage_bucket="sprints-apps",
        zone="us-east1-b"
    )
    proc_nyc_taxi = DataProcPySparkOperator(
        task_id='Processing_NYC_Taxi_Job',
        main='gs://sprints-apps/nyctaxi_trips/app_nyctaxi.py',        
        cluster_name='spark-cluster-{{ds_nodash}}',
        dataproc_pyspark_jars="gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
    )
    delete_cluster = DataprocClusterDeleteOperator(
        task_id="delete_cluster",
        project_id="data-sprints-322619",
        cluster_name="spark-cluster-{{ds_nodash}}",
        trigger_rule="all_done"
    )

create_cluster>>proc_nyc_taxi>>delete_cluster