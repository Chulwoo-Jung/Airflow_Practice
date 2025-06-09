from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id = 'dags_python_with_op_args',
    schedule = None,
    start_date = pendulum.datetime(2025, 1, 1, tz="Europe/Berlin"),
    catchup = False
) as dag:
    
    def regist(name, sex, *args):
        print(f'name: {name}, sex: {sex}')
        print(f'args: {args}')
    
    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable = regist,
        op_args = ['Chulwoo Jung', 'Male', '2000-04-19', 'Berlin']
    )

    python_t1