import pendulum
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_xcom_eg2',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'
    
    @task(task_id='python_xcom_pull_task1')
    def xcom_pull1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(key='return_value', task_ids='python_xcom_push_by_return')
        print(f'value1: {value1}')
    
    @task(task_id='python_xcom_pull_task2')
    def xcom_pull2(status, **kwargs):
        print(f'status: {status}')
    
    xcom_pull2(xcom_push_result())
    xcom_push_result() >> xcom_pull1()