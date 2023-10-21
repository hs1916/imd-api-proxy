from fastapi import FastAPI
from fisis.api import stat_list_get, orgn_list_get, statistic_info_get
from ecos.api import get_ecos_api_data
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

api_key = os.getenv('api_key')
ecos_api_key = os.getenv('ecos_api_key')

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

    :param part_div:
    :return:
    """
    orgn_list = orgn_list_get(part_div, api_key)
    return orgn_list.to_dict(orient='records')


@app.get('/fisis/api-data')
def orgn_list_api_call(finance_cd: str, list_no: str, quarter:str):
    api_data = statistic_info_get(finance_cd, list_no, quarter, api_key)
    api_response = pd.DataFrame(api_data.json()['result']['list'])
    return api_response.to_dict(orient='records')


@app.get('/ecos/api-data')
def ecos_api_call(stat_code: str, cycle: str, start: str, end: str, item_code: str):

    api_data = get_ecos_api_data(stat_code, cycle, start, end, item_code, ecos_api_key)
    api_data_json = api_data.json()

    print('-------------------------')
    print(api_data_json)
    print('-------------------------')

    if 'RESULT' in api_data_json:
        return api_data_json
    else:
        try:
            api_response = pd.DataFrame(api_data_json['StatisticSearch']['row'])
        except:
            print('[ECOS] ecos api call error.... ')

        return api_response.to_dict(orient='records')






