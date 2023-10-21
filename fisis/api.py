import requests
import pandas as pd


def stat_list_get(lrg_div, sml_div_list, api_key):
    """
        통계정보 API
        lrgDiv(금융권역코드)
            H : 생명보험
        smlDiv(통계표 분류코드)
            B : 재무현황
            C : 주요 경영지표
    """
    stat_list = pd.DataFrame()

    try:
        for dvsn in sml_div_list:
            stat_api_url = f'https://fisis.fss.or.kr/openapi/statisticsListSearch.json?lang=kr&auth={api_key}&lrgDiv={lrg_div}&smlDiv={dvsn}'
            print(stat_api_url)
            response = requests.get(stat_api_url)
            temp_df = pd.DataFrame(response.json()['result']['list'])
            stat_list = pd.concat([stat_list, temp_df])
    except:
        print('[FISIS] stat_list get api error... ')

    return stat_list


def orgn_list_get(part_div, api_key):
    """
        금융기관 List API
        partDiv(금융권역코드)
            H : 생명보험
    """
    orgn_api_url = f'https://fisis.fss.or.kr/openapi/companySearch.json?lang=kr&auth={api_key}&partDiv={part_div}'
    finance_orgn_df = pd.DataFrame()

    try:
        response = requests.get(orgn_api_url)
        temp_df = pd.DataFrame(response.json()['result']['list'])
        finance_orgn_df = temp_df[~temp_df['finance_nm'].str.contains('폐')]
    except:
        print('[FISIS] orgn_list get api error... ')

    return finance_orgn_df


def statistic_info_get(finance_cd, list_no, quarter, api_key):
    api_url_prefix = 'http://fisis.fss.or.kr/openapi/statisticsInfoSearch.json?lang=kr'
    info_api_url = f'{api_url_prefix}&auth={api_key}&financeCd={finance_cd}&listNo={list_no}&term=Q&startBaseMm={quarter}&endBaseMm={quarter}'
    response = object()

    try:
        response = requests.get(info_api_url)
    except:
        print(f'[FISIS] statistic_info get api error... {finance_cd}/{list_no}')

    return response