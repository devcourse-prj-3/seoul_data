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


def etl_prediction_data():
    # Excel 파일을 읽어오기
    file_path = '../plugins/서울시 주요113장소명_코드 목록.xlsx'
    data = pd.read_excel(file_path, sheet_name='장소목록')

    # 'AREA_NM' 칼럼의 데이터를 리스트로 변환
    area_names = data['AREA_NM'].tolist()
    service_key = '6f4e49716b79616e36314b416c7543'

    row2 = []

    for area in area_names:
        url = f'http://openapi.seoul.go.kr:8088/{service_key}/xml/citydata_ppltn/1/5/{area}'
        response = requests.get(url=url)
        soup = BeautifulSoup(response.text, 'lxml-xml')
        items = soup.find_all("SeoulRtd.citydata_ppltn")

        for item in items:
            common_row_data = parse(item)
            if common_row_data:
                if item.find("FCST_YN").get_text() == 'Y':
                    predict_data = get_prediction_data(item)
                    for predict in predict_data:
                        row2.append(predict)
                else:
                    predict_data = handle_missing_prediction(item)
                    row2.append(predict_data)

    return row2

@task
def load_prediction_data_to_redshift(schema, table2):
    cur = get_Redshift_connection()

    create_table2_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table2} (
        area_name varchar(50),
        area_code varchar(20),
        fcst_yn varchar(10),
        conn_time timestamp,
        min_predict_population integer,
        max_perdict_population integer,
        PRIMARY KEY (area_name, conn_time)
    );"""

    logging.info(create_table2_sql)
    try:
        cur.execute(create_table2_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    existing_data_sql = f"SELECT area_name, conn_time FROM {schema}.{table2};"
    cur.execute(existing_data_sql)
    existing_data = {(row[0], row[1]) for row in cur.fetchall()}

    new_rows = [row for row in etl_prediction_data() if (row[0], row[3]) not in existing_data]
    new_data = [(r[0], r[1], r[2], r[3].strftime('%Y-%m-%d %H:%M:%S'), r[4], r[5]) for r in new_rows]

    if new_data:
        insert_sql = f"INSERT INTO {schema}.{table2} VALUES %s;"
        placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s)"] * len(new_data))
        full_insert_sql = insert_sql % placeholders

        logging.info(full_insert_sql)
        try:
            cur.execute(full_insert_sql, [item for sublist in new_data for item in sublist])
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise


    with DAG(
        dag_id="predict_population_data_to_Redshift",
        start_date=datetime(2024,1,9),
        schedule="0 1 * * *",
        max_active_runs=1,
        catchup=True,
        default_args={
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
    ) as dag:
        load_prediction_data_to_redshift("diddmstj15", "population_prediction_data")
