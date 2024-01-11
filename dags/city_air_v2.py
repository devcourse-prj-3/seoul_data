from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json


def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


@task
def etl(schema, table, api_key):

    url = f"http://openAPI.seoul.go.kr:8088/{api_key}/json/ListAirQualityByDistrictService/1/25/"
    response = requests.get(url)
    res = json.loads(response.text)
    data = res['ListAirQualityByDistrictService']['row']
  # Todo 여기까지 코드 수정 함
    """
    {'MSRDATE': '202401101000',
    'MSRADMCODE': '111123',
    'MSRSTENAME': '종로구',
    'MAXINDEX': '',
    'GRADE': '',
    'POLLUTANT': '',
    'NITROGEN': '0.034',
    'OZONE': '0.007',
    'CARBON': '0.8',
    'SULFUROUS': '0.003',
    'PM10': '65',
    'PM25': '59'}
    """
    ret = [create_sql_values(row) for row in data]

    cur = get_Redshift_connection()
    
    # 원본 테이블이 없다면 생성
    create_table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
    msr_date DATE,
    msr_time TIME,
    msradmcode CHAR(6),
    msrstename VARCHAR(12),
    maxindex CHAR(4),
    grade VARCHAR(12),
    pollutant VARCHAR(16),
    nitrogen VARCHAR(10),
    ozone VARCHAR(10),
    carbon VARCHAR(10),
    sulfurous VARCHAR(10),
    pm10 VARCHAR(10),
    pm25 VARCHAR(10),
    created_date timestamp DEFAULT GETDATE()
);"""
    logging.info(create_table_sql)

    # 임시 테이블 생성
    create_t_sql = f"""CREATE TEMP TABLE t AS SELECT * FROM {schema}.{table};"""
    logging.info(create_t_sql)
    try:
        cur.execute(create_table_sql)
        cur.execute(create_t_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 임시 테이블 데이터 입력
    insert_sql = f"INSERT INTO t VALUES " + ",".join(ret)
    logging.info(insert_sql)
    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 기존 테이블 대체
    alter_sql = f"""DELETE FROM {schema}.{table};
    INSERT INTO {schema}.{table}
    SELECT msr_date, msr_time, msradmcode, msrstename, maxindex, grade, pollutant, nitrogen, ozone, carbon, sulfurous, pm10, pm25 FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY msr_date, msrstename ORDER BY msr_time DESC) seq
        FROM t
    )
    WHERE seq = 1;"""

    logging.info(alter_sql)
    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

def create_sql_values(dictionary):
    values = []
    for key, value in dictionary.items():
        if key == 'MSRDATE':
            day = datetime.strptime(value, '%Y%m%d%H%M').strftime('%Y-%m-%d %H:%M')
            date, time = day.split(' ')
            values.append(f"'{date}'")
            values.append(f"'{time}'")
        elif isinstance(value, str) and value:
            values.append(f"'{value}'")
        elif not value:
            values.append("NULL")
        else:
            values.append(str(value))
    return "(" + ", ".join(values) + ")"


with DAG(
    dag_id = 'City_air_to_Redshift_v2',
    start_date = datetime(2024,1,9), 
    schedule = '20 * * * *',  
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    etl("jung_hoon_loo", "city_air_v2", Variable.get("seoul_data_api"))
