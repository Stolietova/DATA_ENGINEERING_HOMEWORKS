from datetime import datetime, timedelta
from pendulum import timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email import EmailOperator

from python_scripts.train_model import process_iris_data
from python_scripts.iris_ml_processor import run_inference

DBT_PROJECT_DIR = "dags/dbt/homework/"
DBT_PROFILE_DIR = "dags/dbt/"
DBT_PROFILE_NAME = "homework"
DBT_TARGET_NAME = "dev"

KYIV_TIMEZONE = timezone('Europe/Kiev')

default_args = {
	"owner": "airflow",
	"depends_on_past": False,
	"start_date": datetime(2025, 4, 22, tzinfo=KYIV_TIMEZONE),
    "end_date" : datetime(2025, 4, 25, tzinfo=KYIV_TIMEZONE), #+1 day for 24.04
    "retries": 1,
	"retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="process_iris",
    default_args=default_args,
    schedule="0 1 * * *",
    catchup=True,
    max_active_runs=2
) as dag:
    
    # Створила додаткову таску t0, оскільки виникали помилки при перезапуску контейнера
    t0_dbt_dependencies = BashOperator(
        task_id="dbt_dependencies",
        bash_command=f"dbt deps --project-dir {DBT_PROJECT_DIR}",
        cwd="/opt/airflow",
    )

    # use BashOperator to run dataset transformation
    t1_transform_dataset = BashOperator(
        task_id="transform_dataset",
        bash_command=f"dbt run "
                     f"--project-dir {DBT_PROJECT_DIR} "
                     f"--profiles-dir {DBT_PROFILE_DIR} "
                     f"--profile {DBT_PROFILE_NAME} "
                     f"--target {DBT_TARGET_NAME} "
                     f"--vars '{{\"process_date\": \"{{{{ next_ds }}}}\"}}'",
        cwd="/opt/airflow",
    )
    
    # use PythonOperator to train model
    t2_train_model = PythonOperator(
        task_id="train_model",
        python_callable=process_iris_data,
        op_kwargs={'data_date': '{{ next_ds }}'},
        provide_context=True,
    )

    # send email about successfull status of model training
    t3_email_message_mt = EmailOperator(
        task_id="send_email_model_train",
        to=["your_mail@meta.ua"],
        subject="SUCCESS: IRIS Data Pipeline Run for {{ next_ds }}",
        html_content="Airflow DAG Run for {{ next_ds }} Completed Successfully",

    )

    #ADDITIONAL TASK

    # model inference
    t4_inference_model = PythonOperator(
        task_id="inference_model",
        python_callable=run_inference,
        op_kwargs={'data_date': '{{ next_ds }}'},
        provide_context=True, 
    )

    # send email about successfull status of model inference
    t5_email_message_im = EmailOperator(
        task_id="send_email_model_inference",
        to=["your_mail@meta.ua"],
        subject="[ML Pipeline] Inference Score Report for Data Date {{ next_ds }}",
        html_content="""<p>Airflow DAG Run for {{ next_ds }} Completed Successfully</p>
                         <p>The total number of records scored and saved to the homework.iris_predictions table is:{{ (ti.xcom_pull(task_ids='inference_model', key='return_value') | default({}))['len_scored_records'] | default(0) }}</p>
        """,

    )

t0_dbt_dependencies >> t1_transform_dataset >> t2_train_model >> t3_email_message_mt >> t4_inference_model >> t5_email_message_im
