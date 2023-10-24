import requests
import pandas as pd

# 통계 리스트 조회 constant constant
api_key = 'W5QNW637V68JRGMZNEZ3'
api_url = "https://ecos.bok.or.kr/api"
req_type = 'json'
req_lang = 'kr'
req_start = '1'
req_end = '1000'


def send_ecos_api_data(stat_code: str, cycle: str, start: str, end: str, item_code: str, api_key: str):
    """
        한국은행 API 호출
    """
    service_name = 'StatisticSearch'
    base_url = f'{api_url}/{service_name}/{api_key}/{req_type}/{req_lang}/{req_start}/{req_end}'
    call_api_url = f'{base_url}/{stat_code}/{cycle}/{start}/{end}/{item_code}'
    response = object()

    print('-----------------------------')
    print(call_api_url)
    print('-----------------------------')

    try:
        response = requests.get(call_api_url)
    except:
        print(f'[ECOS] StatisticSearch get api error... {stat_code}/{item_code}/{cycle}')

    return response
