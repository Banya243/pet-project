"""Check the lab infrastructure without implementing the training exercise."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.ssh.operators.ssh import SSHOperator


def check_result():
    row = PostgresHook(postgres_conn_id="warehouse").get_first(
        "SELECT spark_version, input_rows FROM public.environment_check"
    )
    if row != ("3.5.8", 7):
        raise ValueError(f"Unexpected Spark result: {row!r}")
    if not S3Hook(aws_conn_id="minio_s3").check_for_key(
        "environment-check/_SUCCESS", bucket_name="processed"
    ):
        raise ValueError("Spark did not finish writing its Parquet output to MinIO")
    print(f"Environment OK: Spark {row[0]}, {row[1]} input rows, S3 and JDBC work")


with DAG(
    dag_id="environment_check",
    description="MinIO -> Spark over SSH -> MinIO + PostgreSQL",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    tags=["lab"],
) as dag:
    wait_for_input = S3KeySensor(
        task_id="wait_for_input",
        aws_conn_id="minio_s3",
        bucket_name="raw",
        bucket_key="orders/2026-10-26/orders.csv",
        poke_interval=5,
        timeout=120,
        mode="reschedule",
        deferrable=False,
    )

    run_spark = SSHOperator(
        task_id="run_spark",
        ssh_conn_id="spark_ssh",
        command="/usr/local/bin/submit-spark /opt/lab/jobs/pyspark/environment_check.py",
        conn_timeout=30,
        cmd_timeout=300,
        get_pty=True,
        do_xcom_push=False,
    )

    verify_output = PythonOperator(
        task_id="verify_output",
        python_callable=check_result,
    )

    wait_for_input >> run_spark >> verify_output
