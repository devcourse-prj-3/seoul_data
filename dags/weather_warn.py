from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import json


def get_Redshift_connection():
    # autocommit is False by default
    hook = PostgresHook(postgres_conn_id="redshift_dev_db")
    return hook.get_conn().cursor()


@task
def etl(schema, table):
    # api_key = Variable.get("weather_warn_api_key") ### airflow 웹 서버에서 등록하면 수정
    api_key = "3EYl02wVmoctJzA9XpHxw%2BUOoGOrlLXn22DPKh5N89OTiXLjQcmS2wf0ee4F%2FVJNTdQx3us6ONk%2BYC1F0Kc4lw%3D%3D"
    stn_id_names = {
        400: "강남구",
        401: "서초구",
        402: "강동구",
        403: "송파구",
        404: "강서구",
        405: "양천구",
        406: "도봉구",
        407: "노원구",
        408: "동대문구",
        409: "중랑구",
        410: "종로구",
        411: "마포구",
        412: "서대문구",
        413: "광진구",
        414: "성북구",
        415: "용산구",
        416: "은평구",
        417: "금천구",
        419: "중구",
        421: "성동구",
        423: "구로구",
        424: "강북구",
        509: "관악구",
        510: "영등포구",
        889: "동작구",
    }

    stn_id_data = {}

    for stn_id, name in stn_id_names.items():
        url = f"http://apis.data.go.kr/1360000/WthrWrnInfoService/getWthrWrnList?ServiceKey={api_key}&stnId={stn_id}&pageNo=1&dataType=json"
        response = requests.get(url)
        data = json.loads(response.text)

        # 데이터가 있는지 확인하고 저장
        items = data.get("response", {}).get("body", {}).get("items", {}).get("item")
        if items:
            stn_id_data[name] = items
    ret = []
    for guname, values in stn_id_data.items():
        for value in values:
            ret.append(
                "('{}','{}','{}','{}')".format(
                    value["stnId"], guname, value["title"], value["tmFc"]
                )
            )
    cur = get_Redshift_connection()
    drop_recreate_sql = f"""DROP TABLE IF EXISTS {schema}.{table};
CREATE TABLE {schema}.{table} (
    stnId VARCHAR,
    GUNAME VARCHAR,
    title VARCHAR(500),
    tmFc VARCHAR,
    created_date timestamp default GETDATE()
);
"""
    insert_sql = f"""INSERT INTO {schema}.{table} VALUES """ + ",".join(ret)
    logging.info(drop_recreate_sql)
    logging.info(insert_sql)
    try:
        cur.execute(drop_recreate_sql)
        cur.execute(insert_sql)
        cur.execute("Commit;")
    except Exception as e:
        cur.execute("Rollback;")
        raise


with DAG(
    dag_id="weather_warn_to_Redshift",
    start_date=datetime(2024, 1, 9),  # 날짜가 미래인 경우 실행이 안됨
    schedule_interval="2 * * * *",  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
) as dag:
    etl("jang_jungbin", "weather_warn_list")
