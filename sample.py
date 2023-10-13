import time
import pandas as pd
import requests
import os
from tqdm import tqdm
import datetime
import psycopg2
import sqlalchemy
from utils.dateutil import quarter_get

db_endpoint = 'testdb.cd0ub2t5tipq.ap-northeast-2.rds.amazonaws.com:5432/imd'
db_user = os.environ['DATABASE_USER']
db_password = os.environ['DATABASE_PASSWORD']

api_key = os.environ['FISIS_API_KEY']


def db_engine_get():
    """
    database 접근을 위한 DB Engine 생성
    :return:
    """
    db_uri = f'postgresql+psycopg2://{db_user}:{db_password}@{db_endpoint}'
    db_engine = sqlalchemy.create_engine(url=db_uri)
    return db_engine


def stat_list_get(lrgDiv, sml_div_list):
    """
        통계정보 API
        lrgDiv(금융권역코드)
            H : 생명보험
        smlDiv(통계표 분류코드)
            B : 재무현황
            C : 주요 경영지표
    """

    stat_list = pd.DataFrame()

    for dvsn in sml_div_list:
        stat_api_url = f'http://fisis.fss.or.kr/openapi/statisticsListSearch.json?lang=kr&auth={api_key}&lrgDiv={lrgDiv}&smlDiv={dvsn}'
        response = requests.get(stat_api_url)
        temp_df = pd.DataFrame(response.json()['result']['list'])
        stat_list = pd.concat([stat_list, temp_df])

    stat_list.to_csv('./stat_list.csv')

    return stat_list


def orgn_list_get(partDiv):
    """
        금융기관 List API
        partDiv(금융권역코드)
            H : 생명보험
    """
    orgn_api_url = f'http://fisis.fss.or.kr/openapi/companySearch.json?lang=kr&auth={api_key}&partDiv={partDiv}'
    response = requests.get(orgn_api_url)
    temp_df = pd.DataFrame(response.json()['result']['list'])
    finance_orgn_df = temp_df[~temp_df['finance_nm'].str.contains('폐')]
    finance_orgn_df.to_csv('./orgn_list.csv')

    return finance_orgn_df


def api_list_store(stat_list, orgn_list):
    """
        호출 해야하는 (통계목록, 금융기관) 세트 API 수신내역 대상 테이블에 저장
    :param stat_list:
    :param orgn_list:
    :return:
    """
    idx_id_list = []  # API ID (통계목록-금융기관)
    idx_nm_list = []  # (통계목록-금융기관 명 )
    data_lgcf_nm_list = []  # 테이블 컬럼 데이터 대분류 명(=통계목록명)
    data_mdcf_nm_list = []  # 테이블 컬럼 데이터 중분류 명(=금융기관명)

    with tqdm(total=(len(stat_list) * len(orgn_list))) as progress_bar:
        for index, orgn in orgn_list.iterrows():
            for i, stat in stat_list.iterrows():
                idx_id = f'{stat["list_no"]}-{orgn["finance_cd"]}'
                idx_nm = f'{stat["list_nm"]}-{orgn["finance_nm"]}'
                data_lgcf_nm = stat["list_nm"]
                data_mdcf_nm = orgn["finance_nm"]
                idx_id_list.append(idx_id)
                idx_nm_list.append(idx_nm)
                data_lgcf_nm_list.append(data_lgcf_nm)
                data_mdcf_nm_list.append(data_mdcf_nm)

                progress_bar.update(1)

    # 테이블 레이아웃대로 데이터프레임 생성
    api_list_df = pd.DataFrame({'rcve_orgn': 'FISIS',
                                'idx_id': idx_id_list,
                                'rgsr_emnb': 'IMD-BATCH',
                                'rgst_dttm': datetime.datetime.now(),
                                'rgst_prgm_id': 'FISIS-API-RECIEVE',
                                'last_chnr_emnb': 'IMD-BATCH',
                                'last_chng_dttm': datetime.datetime.now(),
                                'last_chng_prgm_id': 'FISIS-API-RECIEVE',
                                'idx_nm': idx_nm_list,
                                'data_lgcf_nm': data_lgcf_nm_list,
                                'data_mdcf_nm': data_mdcf_nm_list,
                                'rgn_cls': '',
                                'idx_peri': 'Q',
                                'unit': '',
                                'rcve_yn': 'Y'
                                })
    # DB 저장
    db_engine = db_engine_get()
    api_list_df.to_sql(
        'tbl_iia_iam_idxinfo',
        db_engine,
        schema='imd_ia',
        if_exists='replace',
        index=False
    )
    db_engine.execute('commit;')
    db_engine.dispose()


def statistic_info_get():
    # 분기 일자
    quarter = quarter_get(datetime.datetime(2023, 4, 13))
    # 통계항목 API Prefix
    api_url_prefix = 'http://fisis.fss.or.kr/openapi/statisticsInfoSearch.json?lang=kr'

    item_list_df = pd.DataFrame()  # 저장을 위한 dataframe

    db_engine = db_engine_get()  # DB Engine
    table_name = 'tbl_iia_iam_idxinfo'  # 조회 테이블
    table_schema = 'imd_ia'  # 조회 스키마

    # API 호출 대상 내역 조회
    select_query = f"select * from {table_schema}.{table_name} where rcve_orgn = 'FISIS' and rcve_yn = 'Y'"
    api_list = pd.read_sql_query(select_query, db_engine)

    # API 호출
    with tqdm(total=len(api_list)) as progress_bar:
        for index, row in api_list.iterrows():
            id_array = row['idx_id'].split('-')
            nm_array = row['idx_nm'].split('-')
            print(id_array)
            print(nm_array)
            info_api_url = f'{api_url_prefix}&auth={api_key}&financeCd={id_array[1]}&listNo={id_array[0]}&term=Q&startBaseMm={quarter}&endBaseMm={quarter}'

            response = requests.get(info_api_url)
            temp_df = pd.DataFrame(response.json()['result']['list'])
            temp_df['list_no'] = id_array[0]
            temp_df['list_nm'] = nm_array[0]
            item_list_df = pd.concat([item_list_df, temp_df])

            time.sleep(0.2)
            progress_bar.update(1)

    # DB 저장
    item_list_df.to_sql(
        'tbl_iia_iam_fisis',
        db_engine,
        schema='imd_ia',
        if_exists='replace', # PK row 있는경우 replace
        index=False
    )


if __name__ == "__main__":
    # stat_list =  stat_list_get(['B', 'C'])
    # orgn_list = orgn_list_get()
    # quater_get(datetime.datetime(2013, 3, 13))
    # quater_get(datetime.datetime(2023, 4, 13))
    # quater_get(datetime.datetime(2023, 7, 13))
    # quater_get(datetime.datetime(2023, 10, 13))
    # quater_get(datetime.datetime(2023, 12, 13))
    # quater_get(datetime.datetime.now())

    statistic_info_get()

    # api_list_store(stat_list=stat_list, orgn_list=orgn_list)

    # db_engine_get()
