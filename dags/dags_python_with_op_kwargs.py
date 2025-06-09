from airflow.sdk import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from common.common_func import regist2, regist

with DAG(
    dag_id = 'dags_python_with_op_kwargs',
    schedule = None,
    start_date = pendulum.datetime(2025, 1, 1, tz="Europe/Berlin"),
    catchup = False
) as dag:
    
    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable = regist2,
        op_args= ['Chulwoo Jung', 'Male','2000-04-19', 'Berlin'],
        op_kwargs = {'email': 'chulwoo.jung@gmail.com', 'phone': '010-1234-5678'}
    )

    python_t2 = PythonOperator(
        task_id = 'python_t2',
        python_callable = regist,
        op_args = ['Chulwoo Jung', 'Male', '2000-04-19', 'Berlin']
    )

    python_t1 >> python_t2