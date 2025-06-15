from airflow.sdk import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator):

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = f'{{var.value.apikey_openapi_seoul_go_kr}}/json/{dataset_nm}'
        self.base_dt = base_dt

    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000

        while True:
            self.log.info(f'start_row: {start_row}, end_row: {end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])
            if len(row_df) < 1000:
                break

            start_row += 1000
            end_row += 1000
        
        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')

        total_row_df.to_csv(f'{self.path}/{self.file_name}.csv', index=False, encoding='utf-8')

    
    def _call_api(self,base_url, start_row, end_row) -> pd.DataFrame:
        import requests
        import json

        headers = {
            'Content-Type': 'application/json',
            'charset': 'utf-8',
            'Accept': '*/*'
        }

        request_url = f'{base_url}/{start_row}/{end_row}/'
        if self.base_dt:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        
        response = requests.get(request_url, headers=headers)
        contents = json.loads(response.text)

        key_nm = list(contents.keys())[0]
        row_data = contents[key_nm]['row']

        return pd.DataFrame(row_data)