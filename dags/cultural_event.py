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


# 작은 따옴표를 이스케이프하여 문자열을 생성하는 함수
def escape_quotes(s):
    return s.replace("'", r"\'")


@task
def etl(schema, table):
    # api_key = Variable.get("cultural_event_info_api_key") ### airflow 웹 서버에서 등록하면 수정
    api_key = "73477765706a626a3131305a73687a53"

    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/culturalEventInfo/1/1000"
    response = requests.get(url)
    data = json.loads(response.text)

    """
    {'CODENAME': '국악', 'GUNAME': '종로구', 'TITLE': '[종로문화재단] K 국악, 판소리가 좋다', 'DATE': '2023-11-01~2023-11-04', 'PLACE': '우리소리도서관 국악누리방', 'ORG_NAME': '종로문화재단', 'USE_TRGT': '누구나 ', 'USE_FEE': '', 'PLAYER': '원미혜, 신성수 명창', 'PROGRAM': '유성기 음반 감상회, 판소리 춘향가 공연 ', 'ETC_DESC': '', 'ORG_LINK': 'https://www.jfac.or.kr/site/main/program/perf_show_list_view?pgIdx=2147', 'MAIN_IMG': 'https://culture.seoul.go.kr/cmmn/file/getImage.do?atchFileId=d1dfed4950d34a1eb45dcfe7270ccbf0&thumb=Y', 'RGSTDATE': '2023-10-21', 'TICKET': '기관', 'STRTDATE': '2023-11-01 00:00:00.0', 'END_DATE': '2023-11-04 00:00:00.0', 'THEMECODE': '가족 문화행사', 'LOT': '37.5744823869', 'LAT': '126.9902329177', 'IS_FREE': '무료', 'HMPG_ADDR': 'https://culture.seoul.go.kr/culture/culture/cultureEvent/view.do?cultcode=143825&menuNo=200008'}
    """
    # 필요한 데이터 추출
    event_list = data["culturalEventInfo"]["row"]

    current_date = datetime.now()
    filtered_events = []

    for event in event_list:
        start_date = datetime.strptime(event["STRTDATE"], "%Y-%m-%d %H:%M:%S.%f")
        end_date = datetime.strptime(event["END_DATE"], "%Y-%m-%d %H:%M:%S.%f")

        if start_date < current_date and end_date >= current_date:
            filtered_events.append(
                {
                    "CODENAME": event["CODENAME"],
                    "GUNAME": event["GUNAME"],
                    "TITLE": event["TITLE"],
                    "DATE": event["DATE"],
                    "PLACE": event["PLACE"],
                    "ORG_NAME": event["ORG_NAME"],
                    "USE_TRGT": event["USE_TRGT"],
                    "USE_FEE": event["USE_FEE"],
                    "PLAYER": event["PLAYER"],
                    "PROGRAM": event["PROGRAM"],
                    "ETC_DESC": event["ETC_DESC"],
                    "ORG_LINK": event["ORG_LINK"],
                    "MAIN_IMG": event["MAIN_IMG"],
                    "RGSTDATE": event["RGSTDATE"],
                    "STRTDATE": event["STRTDATE"],
                    "END_DATE": event["END_DATE"],
                    "TICKET": event["TICKET"],
                    "THEMECODE": event["THEMECODE"],
                    "LOT": event["LOT"],
                    "LAT": event["LAT"],
                    "IS_FREE": event["IS_FREE"],
                    "HMPG_ADDR": event["HMPG_ADDR"],
                }
            )

    ret = []
    for d in filtered_events:
        # 필요한 데이터들을 문자열로 변환하여 리스트에 추가
        event_info = "('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(
            escape_quotes(d["CODENAME"]),
            escape_quotes(d["GUNAME"]),
            escape_quotes(d["TITLE"]),
            escape_quotes(d["DATE"]),
            escape_quotes(d["PLACE"]),
            escape_quotes(d["ORG_NAME"]),
            escape_quotes(d["USE_TRGT"]),
            escape_quotes(d["USE_FEE"]),
            escape_quotes(d["PLAYER"]),
            escape_quotes(d["PROGRAM"]),
            escape_quotes(d["ETC_DESC"]),
            escape_quotes(d["ORG_LINK"]),
            escape_quotes(d["MAIN_IMG"]),
            escape_quotes(d["RGSTDATE"]),
            escape_quotes(d["TICKET"]),
            escape_quotes(d["STRTDATE"]),
            escape_quotes(d["END_DATE"]),
            escape_quotes(d["THEMECODE"]),
            escape_quotes(d["LOT"]),
            escape_quotes(d["LAT"]),
            escape_quotes(d["IS_FREE"]),
            escape_quotes(d["HMPG_ADDR"]),
        )
        ret.append(event_info)

    cur = get_Redshift_connection()
    drop_recreate_sql = f"""DROP TABLE IF EXISTS {schema}.{table};
CREATE TABLE {schema}.{table} (
    CODENAME VARCHAR,
    GUNAME VARCHAR,
    TITLE VARCHAR(500),
    DATE VARCHAR,
    PLACE VARCHAR(500),
    ORG_NAME VARCHAR(500),
    USE_TRGT VARCHAR(500),
    USE_FEE VARCHAR(500),
    PLAYER VARCHAR,
    PROGRAM VARCHAR(500),
    ETC_DESC VARCHAR(500),
    ORG_LINK VARCHAR(500),
    MAIN_IMG VARCHAR(500),
    RGSTDATE VARCHAR,
    TICKET  VARCHAR,
    STRTDATE VARCHAR,
    END_DATE VARCHAR,
    THEMECODE VARCHAR,
    LOT VARCHAR,
    LAT VARCHAR,
    IS_FREE VARCHAR,
    HMPG_ADDR VARCHAR(500),
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
    dag_id="cultural_event_to_Redshift",
    start_date=datetime(2024, 1, 8),  # 날짜가 미래인 경우 실행이 안됨
    schedule="0 2 * * *",  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
) as dag:
    etl("jang_jungbin", "cultural_event")
