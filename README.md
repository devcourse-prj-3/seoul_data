
# 서울시 문화행사를 위한 실시간 데이터 파이프라인

## 프로젝트 내용
 서울시에서 제공하는 문화행사를 기반으로 각 위치 별 실시간 데이터를 조회 서비스



## 1. Configuration

- OS: Ubuntu22.04
- Docker : 24.0.4
- Data warehouse : Amazon Redshift
- Airflow : 2.5.1
- Schedule interval: 1시간
- Data source
    - [실시간 인구 데이터](https://data.seoul.go.kr/dataList/OA-21778/A/1/datasetView.do)
    - [문화생활 데이터](https://data.seoul.go.kr/dataList/OA-15486/S/1/datasetView.do)
    - [대기환경 데이터](https://data.seoul.go.kr/dataList/OA-1200/S/1/datasetView.do)
    - [기상청 단기예보](https://www.data.go.kr/data/15084084/openapi.do#tab_layer_detail_function)

## 2. Architecture

![architecture](/images/architecture.png)

## 3. Database

<details>
<summary>데이터 테이블 구조 넣어주기</summary>
 <div>
  <ul>
    <li><p>01_population_redshift_in_v2</p>
        <img src="/images/01_population_redshift_in_v2.png" alt="01_population_redshift_in_v2"></li>
    <li><p>02_predict_redshift_in_3</p>
        <img src="/images/02_predict_redshift_in_3.png" alt="02_predict_redshift_in_3"></li>
    <li><p>03_city_air_v2</p>
        <img src="/images/03_city_air_v2.png" alt="03_city_air_v2"></li>
    <li><p>04_cultural_event</p>
        <img src="/images/04_cultural_event.png" alt="04_cultural_event"></li>
    <li><p>05_weather_warn_list</p>
        <img src="/images/05_weather_warn_list.png" alt="05_weather_warn_list"></li>
    <li><p>06_ultra_srt_ncst</p>
        <img src="/images/06_ultra_srt_ncst.png" alt="06_ultra_srt_ncst"></li>
    <li><p>07_region_nx_ny</p>
        <img src="/images/07_region_nx_ny.png" alt="07_region_nx_ny"></li>
  </ul>
 </div>
</details> 
    

## 4. 실행방법

*실행 전 `git`, `docker`, `docker-compose` 설치 필요*

1. 해당 repository 복사 후 진입
    
    ```bash
    git clone
    cd seoul_data
    ```
    
2. airflow를 실행하기 전에 현재 사용자의 UID를 .env파일에 설정함
이 작업은 Docker 컨테이너와 호스트 시스템 간의 파일 권한 문제를 해결하기 위해 필요(권장)
    
    ```bash
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```
    
3. Airflow 초기화 (처음 설정할 때만 필요)**
초기 데이터베이스 구조 설정 및 기본 사용자 생성
    
    ```bash
    docker-compose up airflow-init
    ```
    
4. 실행
    
    ```bash
    docker-compose up -d
    ```
    
5.  **Airflow Web UI**
   [http://localhost:8080](http://localhost:8080/) 
6. 종료 
    
    ```bash
    docker-compose down
    ```
    

### 조회

- sql/ 에서 원하는 query로 조회 가능
