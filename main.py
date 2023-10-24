from fastapi import FastAPI
from fisis.api import stat_list_get, orgn_list_get, statistic_info_get
from kosis.api import send_kosis_api
from ecos.api import send_ecos_api_data
from kof.api import send_std_yield, send_call_yield, send_fund_std_val
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

api_key = os.getenv('api_key')
ecos_api_key = os.getenv('ecos_api_key')
kosis_api_key = os.getenv('kosis_api_key')

app = FastAPI()


@app.get('/fisis/stat-list')
def stat_list_api_call(lrg_div: str, sml_div_list: str):
    """

    [fisis002] 금융감독원 통계정보 API 호출 Layer

    :param lrg_div: 금융기관 구분
    :param sml_div_list: 통계표 분류코드
    :return: 금융기관별 통계표
    """

    stat_list = stat_list_get(lrg_div, eval(sml_div_list), api_key)
    return stat_list.to_dict(orient='records')


@app.get('/fisis/orgn-list')
def orgn_list_api_call(part_div: str):
    """

    [fisis001] 금융감독원 금융기관 정보 API 호출 Layer

    :param part_div: 금융기관 분류 (H: 생명보험)
    :return: 금융기관
    """

    orgn_list = orgn_list_get(part_div, api_key)
    return orgn_list.to_dict(orient='records')


@app.get('/fisis/api-data')
def orgn_list_api_call(finance_cd: str, list_no: str, quarter: str):
    """
    [fisis003] 금융감독원 api 데이터 조회

    :param finance_cd: 금융기관 코드
    :param list_no: 통계 번호
    :param quarter: 데이터 주기
    :return: api 데이터
    """

    api_data = statistic_info_get(finance_cd, list_no, quarter, api_key)
    api_response = pd.DataFrame(api_data.json()['result']['list'])
    return api_response.to_dict(orient='records')


@app.get('/ecos/api-data')
def ecos_api_call(stat_code: str, cycle: str, start: str, end: str, item_code: str):
    """
    [ecos001] 한국은행 api 데이터 조회

    :param stat_code: 통계코드
    :param cycle: 주기
    :param start: 시작시점
    :param end: 종료시점
    :param item_code: 통계코드별 아이템 코드
    :return: api 데이터
    """
    api_data = send_ecos_api_data(stat_code, cycle, start, end, item_code, ecos_api_key)
    api_data_json = api_data.json()

    if 'RESULT' in api_data_json:
        return api_data_json
    else:
        try:
            api_response = pd.DataFrame(api_data_json['StatisticSearch']['row'])
        except:
            print('[ECOS] ecos api call error.... ')

        return api_response.to_dict(orient='records')


@app.get("/kosis/api-data")
def kosis_api_call(prd_de: str, jipyo_id: str):
    """

    [kosis001] 통계청 api 데이터 조회

    :param prd_de: period 값
    :param jipyo_id: 지표ID
    :return: api 데이터
    """

    api_data = send_kosis_api(prd_de, jipyo_id, kosis_api_key)
    api_data_json = api_data.json()
    return api_data_json

@app.get("/kof/std-yield")
def call_kof_std_yield(base_date: str):
    """

    [kofia003] 금융투자협회 기준수익률 크롤링

    :param base_date: 기준일자
    :return: 채권 기준 수익률
    """
    response = object()
    try:
        response = send_std_yield(base_date)
    except:
        print('[KOF] kof std-yield api call error.... ')
    return response


@app.get("/kof/call-yield")
def call_kof_call_yield(base_date: str):
    """

    [kofia001] 금융투자협회 호가 수익률

    :param base_date: 기준일자
    :return: 호가수익률
    """
    response = object()
    try:
        response = send_call_yield(base_date)
    except:
        print('[KOF] kof call  -yield api call error.... ')
    return response



@app.get("/kof/fund-std-val")
def call_kof_call_yield(base_date: str):
    """

    [kofia002] 금융투자협회 펀드기준가

    :param base_date: 기준일자
    :return: 펀드기준가
    """
    response = object()
    try:
        response = send_fund_std_val(base_date)
    except:
        print('[KOF] kof fund-std-val api call error.... ')
    return response
