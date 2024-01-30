from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def _create_table(cur, schema, table, drop_first):
    if drop_first:
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
    cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    base_date date,
                    base_time time,
                    category varchar(8),
                    nx integer,
                    ny integer,
                    obsr_value DOUBLE PRECISION,
                    created_date timestamp DEFAULT dateadd(hour,9,GETDATE())
                );""")

@task
def extract(base_url, encoding_key):
    logging.info("extract started")
    try:
        demand_type = "getUltraSrtNcst"
        numOfRows = "1000"
        pageNo = "2"
        dataType = "json"

        base_date = str((datetime.now() + timedelta(hours=9)).date().strftime("%Y%m%d"))
        base_time = str((datetime.now() + timedelta(hours=9)).time().strftime("%H%M"))

        nxs_nys = [ (58, 125),
                (58, 126),
                (59, 124),
                (59, 125),
                (59, 127),
                (60, 126),
                (60, 127),
                (61, 125),
                (61, 126),
                (61, 127),
                (61, 128),
                (61, 129),
                (62, 126),
                (62, 128)]
        
        serialized_data_list = []
        
        for nx, ny in nxs_nys:
            nx = str(nx)
            ny = str(ny)
            url =   base_url + "/" + demand_type + \
                    "?serviceKey=" + encoding_key + \
                    "&numOfRows=" + numOfRows + "&pageNo=" + pageNo + "&dataType=" + dataType + \
                    "&base_date=" + base_date + "&base_time=" + base_time + \
                    "&nx=" + nx + "&ny=" + ny

            response = requests.get(url)
            if response.status_code == 200:
                serialized_data = json.dumps(response.json())
                serialized_data_list.append(serialized_data)
            else:
                print(response.status_code)
                logging.info("response status code error")
                raise

        logging.info("extract done")
        return serialized_data_list
        
    except (Exception) as error:
        print(error)
        logging.info("extract error")
        raise


@task
def transform(serialized_data_list):
    logging.info("Transform started")
    final_records = []
    
    for serialized_data in serialized_data_list:
        pulled_data = json.loads(serialized_data)
        items = pulled_data["response"]["body"]["items"]["item"]
        records = []
        for item in items:
            record = tuple(item.values())
            records.append(record)
            
        final_records += records
        
    logging.info("Transform done")
    return final_records


@task
def load(schema, table, records):
    logging.info("load started")    
    cur = get_Redshift_connection()   
    """
    records = [
      [ "20240109", "0600", "PTY", 55, 127, "0" ],
      [ "20240109", "0600", "REH", 55, 127, "79" ],
      ...
    ]
    """
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    try:
        cur.execute("BEGIN;")
        # 원본 테이블이 없으면 생성 - 테이블이 처음 한번 만들어질 때 필요한 코드
        _create_table(cur, schema, table, False)
        # 임시 테이블로 원본 테이블을 복사
        create_t_sql = f""" CREATE TEMP TABLE t (LIKE {schema}.{table} INCLUDING DEFAULTS);
                            INSERT INTO t SELECT * FROM {schema}.{table};"""
        cur.execute(create_t_sql)
        
        for r in records:
            base_date = r[0]
            base_time = r[1]
            category = r[2]
            nx = r[3]
            ny = r[4]
            obsr_value = r[5]
            print(base_date, "-", base_time, "-", category, "-", nx, "-", ny, "-", obsr_value)
            sql = f"INSERT INTO t VALUES ('{base_date}', '{base_time}', '{category}', '{nx}', '{ny}', '{obsr_value}')"
            cur.execute(sql)
        
        # 임시 테이블 내용을 원본 테이블로 복사
        cur.execute(f"DELETE FROM {schema}.{table};")
        cur.execute(f"""INSERT INTO {schema}.{table}
                        SELECT base_date, base_time, category, nx, ny, obsr_value, created_date FROM (
                            SELECT *, ROW_NUMBER() OVER (PARTITION BY base_date, base_time, category, nx, ny ORDER BY created_date DESC) seq
                            FROM t
                        )
                        WHERE seq = 1;""")
        
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except Exception as error:
        print(error)
        cur.execute("ROLLBACK;")
        raise   
    logging.info("load done")


with DAG(
    dag_id='UltraSrtNcst_to_Redshift',
    start_date=datetime(2024, 1, 9),  # 날짜가 미래인 경우 실행이 안됨
    schedule='45 * * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    url = Variable.get("ultra_srt_ncst_api_url")
    encoding_key = Variable.get("ultra_srt_ncst_api_key")
    schema = 'hjm507'   ## 자신의 스키마로 변경
    table = 'ultra_srt_ncst'
    
    lines = transform(extract(url, encoding_key))
    load(schema, table, lines)