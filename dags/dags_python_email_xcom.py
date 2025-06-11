from airflow.sdk import DAG, task
from airflow.providers.smtp.operators.smtp import EmailOperator
import pendulum

with DAG(
    dag_id='dags_python_email_xcom',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2025, 6, 1, tz='Europe/Berlin'),
) as dag:
    
    @task(task_id='some_logic')
    def some_logic(**kwargs):
        from random import choice
        return choice(['그렇습니다', '아닙니다'])
    

    send_email = EmailOperator(
        task_id = 'send_email',
        to = 'diazepam57@gmail.com',
        subject = '{{ data_interval_end.in_timezone("Europe/Berlin") | ds }} 수민이는 바보인가요?',
        html_content = '{{ data_interval_end.in_timezone("Europe/Berlin") | ds }} , 오늘은 <br> \
                        {{ ti.xcom_pull(key="return_value", task_ids="some_logic")}} <b>하하</b>'
    )

    some_logic() >> send_email