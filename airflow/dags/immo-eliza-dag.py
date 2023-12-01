from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
import os
from extract import houses_scraper, apartments_scraper


dag_path = os.getcwd()


def houses_scraper_task():
    houses_scraper()


def apartments_scraper_task():
    apartments_scraper()


def data_cleaning_analysis_task():
    pass


def analysis_dashboard_task():
    pass


def data_cleaning_training_task():
    pass


def training_model_task():
    pass


with DAG(
    dag_id="immo-eliza", start_date=datetime(2023, 12, 1), schedule_interval="@daily"
) as dag:
    with TaskGroup("scraping", tooltip="task group #1") as section_1:
        scraping_houses = PythonOperator(
            task_id="scraping_house", python_callable=houses_scraper_task
        )
        scraping_apartmetns = PythonOperator(
            task_id="scraping_apartments", python_callable=apartments_scraper_task
        )

    with TaskGroup("data_cleaning", tooltip="data cleaning") as section_2:
        data_cleaning_training = PythonOperator(
            task_id="training_cleaning", python_callable=data_cleaning_training_task
        )
        data_cleaning_analysis_dag = PythonOperator(
            task_id="analysis_cleaning", python_callable=data_cleaning_analysis_task
        )

    with TaskGroup(
        "training_and_dashboard", tooltip="training model & dashboard"
    ) as section_3:
        training_dag = PythonOperator(
            task_id="training_model", python_callable=training_model_task
        )
        analysis_dashboard_dag = PythonOperator(
            task_id="dashboard", python_callable=analysis_dashboard_task
        )

    section_1 >> section_2 >> section_3
