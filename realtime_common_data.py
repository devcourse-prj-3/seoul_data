from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

import pandas as pd
import requests
import logging
from bs4 import BeautifulSoup

from plugins.get_population_density import *


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def etl_common_data(schema, table1):
    # Excel 파일을 읽어오기
    file_path = '../plugins/서울시 주요113장소명_코드 목록.xlsx'
    data = pd.read_excel(file_path, sheet_name='장소목록')

    # 'AREA_NM' 칼럼의 데이터를 리스트로 변환
    area_names = data['AREA_NM'].tolist()
    service_key = '6f4e49716b79616e36314b416c7543'

    row1 = []

    for area in area_names:
        url = f'http://openapi.seoul.go.kr:8088/{service_key}/xml/citydata_ppltn/1/5/{area}'
        response = requests.get(url=url)
        soup = BeautifulSoup(response.text, 'lxml-xml')
        items = soup.find_all("SeoulRtd.citydata_ppltn")

        for item in items:
            common_row_data = parse(item)
            if common_row_data:
                row1.append(common_row_data)

    return row1


## full refresh
@task
def load_common_data_to_redshift(schema, table1):
    cur = get_Redshift_connection()

    recreate_table1_sql = f"""
    DROP TABLE IF EXISTS {schema}.{table1};
    CREATE TABLE IF NOT EXISTS {schema}.{table1} (
        area_name varchar(50) primary key,
        area_code varchar(20),
        area_congest varchar(20),
        area_congest_msg varchar(100),
        area_population_min integer,
        area_population_max integer,
        male_rate float,
        female_rate float,
        conn_time timestamp,
        fcst_yn varchar(10)
    );"""

    logging.info(recreate_table1_sql)
    try:
        cur.execute(recreate_table1_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    rows = etl_common_data()
    data = [tuple(row[:8]+[row[8].strftime('%Y-%m-%d %H:%M:%S')]+row[9]) for row in rows]

    insert_sql = f"INSERT INTO {schema}.{table1} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    logging.info(insert_sql)
    try:
        cur.executemany(recreate_table1_sql, data)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    with DAG(
        dag_id="common_population_data_to_Redshift",
        start_date=datetime(2024,1,9),
        schedule="0 1 * * *",
        max_active_runs=1,
        catchup=False,
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
    ) as dag:
        load_common_data_to_redshift("diddmstj15", "population_common_data")

