from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
import pendulum
from airflow.sdk import DAG


with DAG(
    dag_id='dags_seoul_api_corona',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2025,6,1, tz='Europe/Berlin'),
    catchup=False,
    tags=['seoul', 'corona', 'api']
) as dag:

    # Task to get corona case count data
    tb_corona19_count_status = SeoulApiToCsvOperator(
        task_id='tb_corona19_count_status',
        dataset_nm='TbCorona19CountStatus',
        path='files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Europe/Berlin") | ds_nodash }}',
        file_name='TbCorona19CountStatus.csv'
    )

    # Task to get vaccination statistics data  
    tv_corona19_vaccine_stat_new = SeoulApiToCsvOperator(
        task_id='tv_corona19_vaccine_stat_new',
        dataset_nm='tvCorona19VaccinestatNew',
        path='files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Europe/Berlin") | ds_nodash }}',
        file_name='tvCorona19VaccinestatNew.csv'
    )

    # Define task dependencies
    tb_corona19_count_status >> tv_corona19_vaccine_stat_new