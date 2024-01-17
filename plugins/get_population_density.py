# 예측값 존재 여부 상관 없이 공통으로 불러올 내용 파싱하기
def parse(item):
    try:
        AREA_NAME = item.find("AREA_NM").get_text()
        AREA_CODE = item.find("AREA_CD").get_text()
        AREA_CONGEST = item.find("AREA_CONGEST_LVL").get_text()
        AREA_CONGEST_MSG = item.find("AREA_CONGEST_MSG").get_text()
        AREA_PPLTN_MIN = item.find("AREA_PPLTN_MIN").get_text()
        AREA_PPLTM_MAX = item.find("AREA_PPLTN_MAX").get_text()
        MALE_PPLTN_RATE = item.find("MALE_PPLTN_RATE").get_text()
        FEMALE_PPLTN_RATE = item.find("FEMALE_PPLTN_RATE").get_text()
        PPLTN_TIME = item.find("PPLTN_TIME").get_text()
        FCST_YN = item.find("FCST_YN").get_text()
        return {
                "장소명": AREA_NAME,
                "장소 코드": AREA_CODE,
                "장소 혼잡도": AREA_CONGEST,
                "장소 혼잡도 메세지": AREA_CONGEST_MSG,
                "인구 지표 최솟값": AREA_PPLTN_MIN,
                "인구 지표 최댓값": AREA_PPLTM_MAX,
                "남성 인구 비율": MALE_PPLTN_RATE,
                "여성 인구 비율": FEMALE_PPLTN_RATE,
                "API 접속 시간": PPLTN_TIME,
                "예측값 제공 여부": FCST_YN
            }
    except AttributeError as e:
        return {
            "장소명": None,
            "장소 코드": None,
            "장소 혼잡도": None,
            "장소 혼잡도 메세지": None,
            "인구 지표 최솟값": None,
            "인구 지표 최댓값": None,
            "남성 인구 비율": None,
            "여성 인구 비율": None,
            "API 접속 시간": None,
            "예측값 제공 여부": None
        }


# FCST_YN == Y일 때 현재부터 12시간 후까지, 총 12번의 데이터 추가로 불러오기
def get_prediction_data(item):
    predict = []
    fcst_ppltn_list = item.find_all('FCST_PPLTN')

    for pre in fcst_ppltn_list:
        try:
            AREA_NAME = item.find("AREA_NM").get_text()
            AREA_CODE = item.find("AREA_CD").get_text()
            FCST_YN = item.find("FCST_YN").get_text()
            FCST_TIME = pre.find('FCST_TIME').get_text()
            FCST_CONGEST_LVL = pre.find('FCST_CONGEST_LVL').get_text()
            FCST_PPLTN_MIN = pre.find('FCST_PPLTN_MIN').get_text()
            FCST_PPLTN_MAX = pre.find('FCST_PPLTN_MAX').get_text()
            predict.append({
                "장소명": AREA_NAME,
                "장소 코드": AREA_CODE,
                "예측값 제공 여부": FCST_YN,
                "예측 시간": FCST_TIME,
                "장소 예측 혼잡도": FCST_CONGEST_LVL,
                "예측 인구 지표 최솟값": FCST_PPLTN_MIN,
                "예측 인구 지표 최댓값": FCST_PPLTN_MAX
            })
        except AttributeError as e:
            predict.append({
                "장소명": None,
                "장소 코드": None,
                "예측값 제공 여부": None,
                "예측 시간": None,
                "장소 예측 혼잡도": None,
                "예측 인구 지표 최솟값": None,
                "예측 인구 지표 최댓값": None
            })
    return predict


def handle_missing_prediction(item):
    predict = {
        "장소명": item.find("AREA_NM").get_text(),
        "장소 코드": item.find("AREA_CD").get_text(),
        "예측값 제공 여부": item.find("FCST_YN").get_text(),
        "예측 시간": None,
        "장소 예측 혼잡도": None,
        "예측 인구 지표 최솟값": None,
        "예측 인구 지표 최댓값": None
    }
    return predict
