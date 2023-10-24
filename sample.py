import time
import pandas as pd
import requests
import os
from tqdm import tqdm
import datetime
import psycopg2
import sqlalchemy
from utils.dateutil import quarter_get
from utils.database import update_row
import json

from fastapi import FastAPI
from fisis.api import stat_list_get, orgn_list_get, statistic_info_get

app = FastAPI()

db_endpoint = 'testdb.cd0ub2t5tipq.ap-northeast-2.rds.amazonaws.com:5432/imd'
db_user = 'postgres'
db_password = 'postgres1%21'

host_url = "http://127.0.0.1:8000/"


def db_engine_get():
    """
    database 접근을 위한 DB Engine 생성
    :return:
    """
    db_uri = f'postgresql+psycopg2://{db_user}:{db_password}@{db_endpoint}'
    db_engine = sqlalchemy.create_engine(url=db_uri)
    return db_engine



def get_stat_list(lrg_div: str, sml_div_list: list):
    """
    [FISIS002] 금융감독원 통계 리스트 조회
    """
    api_url = f'{host_url}fisis/stat-list?lrg_div={lrg_div}&sml_div_list={str(sml_div_list)}'
    response = requests.get(api_url)
    stat_list = pd.DataFrame(response.json())
    return stat_list


def get_orgn_list(part_div: str):
    """
    [FISIS001] 금융감독원 금융기관 조회
    """
    api_url = f'{host_url}fisis/orgn-list?part_div={part_div}'
    response = requests.get(api_url)
    orgn_list = pd.DataFrame(response.json())
    return orgn_list


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
                                'rgst_prgm_id': 'FISIS-API-RECEIVE',
                                'last_chnr_emnb': 'IMD-BATCH',
                                'last_chng_dttm': datetime.datetime.now(),
                                'last_chng_prgm_id': 'FISIS-API-RECEIVE',
                                'idx_nm': idx_nm_list,
                                'data_lgcf_nm': data_lgcf_nm_list,
                                'data_mdcf_nm': data_mdcf_nm_list,
                                'rgn_cls': '',
                                'idx_peri': 'Q',
                                'unit': '',
                                'rcve_yn': 'N'
                                })
    # DB 저장
    db_engine = db_engine_get()
    api_list_df.to_sql(
        'tbl_iia_iam_idxinfo',
        db_engine,
        schema='imd_ia',
        if_exists='append',
        index=False
    )
    db_engine.execute('commit;')
    db_engine.dispose()


def call_api_info_get(rcve_orgn: str, idx_peri: str = None):
    db_engine = db_engine_get()  # DB Engine
    table_name = 'tbl_iia_iam_idxinfo'  # 조회 테이블
    table_schema = 'imd_ia'  # 조회 스키마

    # API 호출 대상 내역 조회
    select_query = f"select * from {table_schema}.{table_name} where rcve_orgn = {rcve_orgn} and rcve_yn = 'Y'"

    # 주기별 조건이 있을때
    if idx_peri is not None:
        select_query += f' and idx_peri = {idx_peri}'

    print('-------------------------')
    print(select_query)
    print('-------------------------')

    api_list = pd.read_sql_query(select_query, db_engine)
    db_engine.dispose()

    return api_list


def stat_info_get():
    """
    [FISIS003] 금융감독원 API 데이터 조회
    """
    # 분기 일자
    quarter = quarter_get(datetime.datetime(2023, 4, 13))

    item_list_df = pd.DataFrame()  # 저장을 위한 dataframe

    api_list = call_api_info_get("'FISIS'") # API List 조회

    # API 호출
    with tqdm(total=len(api_list)) as progress_bar:
        for index, row in api_list.iterrows():
            id_array = row['idx_id'].split('-')
            nm_array = row['idx_nm'].split('-')
            api_url = f'{host_url}fisis/api-data?finance_cd={id_array[1]}&list_no={id_array[0]}&quarter={str(quarter)}'
            try:
                response = requests.get(api_url)
                temp_df = pd.DataFrame(response.json())
            except:
                print(response.text)
                continue

            temp_df['list_no'] = id_array[0]
            temp_df['list_nm'] = nm_array[0]
            item_list_df = pd.concat([item_list_df, temp_df])

            time.sleep(0.2)
            progress_bar.update(1)

    json_response = item_list_df.to_json(orient='records')

    return json_response(content=json_response)



def ecos_api_list_store():
    """
    한국은행 API 리스트 저장
    :return:
    """
    api_list = pd.read_excel('./api_detail_item_list_rcve.xlsx')

    idx_id_list = []  # API ID : [STAT_CODE]-[CYCLE]-[ITEM_CODE]
    idx_nm_list = []  # API_NAME : [ITEM_NAME]
    idx_peri = []  # 지표 주기 : [CYCLE]
    unit = []  # 단위 [UNIT_NAME]
    data_lgcf_nm_list = []  # [STAT_NAME]
    data_mdcf_nm_list = []  # [P_ITEM_NAME]
    rcve_yn_list = []

    with tqdm(total=(len(api_list))) as progress_bar:
        for index, row in api_list.iterrows():
            idx_id = f'{row["STAT_CODE"]}-{row["CYCLE"]}-{row["ITEM_CODE"]}'
            idx_id_list.append(idx_id)
            idx_nm_list.append(row["ITEM_NAME"])
            idx_peri.append(row["CYCLE"])
            unit.append(row["UNIT_NAME"])
            data_lgcf_nm_list.append(row["STAT_NAME"])
            data_mdcf_nm_list.append(row["P_ITEM_NAME"])
            rcve_yn_list.append(row["RCVE_YN"])
            progress_bar.update(1)

    # 테이블 레이아웃대로 데이터프레임 생성
    api_list_df = pd.DataFrame({'rcve_orgn': 'BOK',
                                'idx_id': idx_id_list,
                                'rgsr_emnb': 'IMD-BATCH',
                                'rgst_dttm': datetime.datetime.now(),
                                'rgst_prgm_id': 'BOK-API-LIST',
                                'last_chnr_emnb': 'IMD-BATCH',
                                'last_chng_dttm': datetime.datetime.now(),
                                'last_chng_prgm_id': 'BOK-API-LIST',
                                'idx_nm': idx_nm_list,
                                'data_lgcf_nm': data_lgcf_nm_list,
                                'data_mdcf_nm': data_mdcf_nm_list,
                                'rgn_cls': '',
                                'idx_peri': idx_peri,
                                'unit': unit,
                                'rcve_yn': rcve_yn_list
                                })

    api_list_df.to_excel('./check_api.xlsx')

    db_engine = db_engine_get()
    api_list_df.to_sql(
        'tbl_iia_iam_idxinfo',
        db_engine,
        schema='imd_ia',
        if_exists='append',
        index=False
    )
    db_engine.execute('commit;')
    db_engine.dispose()


def seperator_period(period, date_value):
    """
    date_value 날짜에 해당하는 Period 구분값 조회
    """
    result = str()

    if period == 'A':
        result = date_value[:4]
    elif period == 'M':
        result = date_value[:6]
    elif period == 'D':
        result = date_value
    elif period == 'Q':
        if date_value[4:6] == '01' or date_value[4:6] == '02' or date_value[4:6] == '03':
            result = date_value[:4] + '1Q'
        elif date_value[4:6] == '04' or date_value[4:6] == '05' or date_value[4:6] == '06':
            result = date_value[:4] + '2Q'
        elif date_value[4:6] == '07' or date_value[4:6] == '08' or date_value[4:6] == '09':
            result = date_value[:4] + '3Q'
        elif date_value[4:6] == '10' or date_value[4:6] == '11' or date_value[4:6] == '12':
            result = date_value[:4] + '4Q'
    elif period == 'S' or period == 'SM':
        if int(date_value[4:6]) <= 6:
            result = date_value[:4] + '1S'
        else:
            result = date_value[:4] + '2S'

    return result


def ecos_start_date(cycle: str, base_date: str):
    """
        한국은행 api 배치 수행시 기준일자 대비 cycle 별 시작 일자 조회
        M : 4개월 전 ~ 전월
        D : 5일 전 ~ 당일
        A : 2년전 ~ 전년
        Q : 6개월전 ~ 당분기
    """

    diff_days = 0

    if cycle == 'D':
        diff_days = 5
    elif cycle == 'A':
        diff_days = 730
    elif cycle == 'Q':
        diff_days = 180
    elif cycle == 'M':
        diff_days = 120
    else:
        diff_days = 180

    date_obj = datetime.datetime.strptime(base_date, '%Y%m%d')
    calculate_date = date_obj - datetime.timedelta(days=diff_days)

    return calculate_date.strftime('%Y%m%d')


def call_ecos_api(cycle: str, base_date: str):
    """
    [ECOS001] 한국은행 API 데이터 조회
    """

    # 1. api list 조회
    api_list = call_api_info_get("'BOK'", cycle)

    # 2. cycle 에 따른 기간 설정
    remove_delimite_cycle = cycle.replace("'", "")
    end = seperator_period(remove_delimite_cycle, base_date)
    start_date = ecos_start_date(remove_delimite_cycle, base_date)
    start = seperator_period(remove_delimite_cycle, start_date)

    api_data_df = pd.DataFrame()

    # 3. api call
    with tqdm(total=(len(api_list))) as progress_bar:
        for index, row in api_list.iterrows():

            parse_arr = row['idx_id'].split('-')

            api_url = f"{host_url}ecos/api-data?stat_code={parse_arr[0]}&cycle={row['idx_peri']}&start={start}&end={end}&item_code={parse_arr[2]}"
            response = requests.get(api_url)

            if 'RESULT' not in response.json():

                print('---------------------')
                print(type(response.json()))
                print(response.json())
                print('---------------------')

                temp_df = pd.DataFrame(response.json())
                print(temp_df)
                api_data_df = pd.concat([api_data_df, temp_df])

            time.sleep(0.5) # 3분에 300개 하면 너무 많은 호출이라고 오류남
            progress_bar.update(1)

    api_data_df.to_excel(f'./ecos_data_{remove_delimite_cycle}.xlsx')

    reload_df = pd.read_excel(f'./ecos_data_{remove_delimite_cycle}.xlsx')
    print(reload_df.info())
    json_response = api_data_df.to_json(orient='records')

    return json_response(content=json_response)


def call_kosis_api(base_date: str):
    """
    [KOSIS001] 통계청 API 호출
    """
    kosis_df = call_api_info_get("'KOSIS'")
    print(kosis_df)

    api_data = pd.DataFrame()

    with tqdm(total=len(kosis_df[811:815])) as progress_bar:
        for index, row in kosis_df[811:815].iterrows():
            prdDe = seperator_period(row['idx_peri'],base_date)

            api_url = f"{host_url}kosis/api-data?prd_de={prdDe}&jipyo_id={row['idx_id']}"

            response = requests.get(api_url)

            if 'err' not in response.json():
                temp_df = pd.DataFrame(response.json())
                api_data = pd.concat([api_data, temp_df])

            time.sleep(0.33)
            progress_bar.update(1)

    api_data.to_excel('./kosis_api_data.xlsx')



def save_kosis_api_list():
    kosis_list = pd.read_excel('./kosis_list.xlsx')
    print(kosis_list)

    engine = db_engine_get()

    kosis_list.to_sql(
        'tbl_iia_iam_idxinfo',
        engine,
        schema='imd_ia',
        if_exists='append',
        index=False
    )
    engine.execute('commit;')
    engine.dispose()


if __name__ == "__main__":
    # stat_list = get_stat_list('H', ['B', 'C'])
    # orgn_list = get_orgn_list('H')
    # quater_get(datetime.datetime(2013, 3, 13))
    # quater_get(datetime.datetime(2023, 4, 13))
    # quater_get(datetime.datetime(2023, 7, 13))
    # quater_get(datetime.datetime(2023, 10, 13))
    # quater_get(datetime.datetime(2023, 12, 13))
    # quater_get(datetime.datetime.now())

    # stat_info_get()

    # api_list_store(stat_list=stat_list, orgn_list=orgn_list)

    # db_engine_get()

    # ecos_api_list_store()

    # api_list = call_api_info_get("'BOK'")
    # api_list.to_excel('./api_bok_list.xlsx')

    # call_ecos_api("'D'", '20231017')

    # call_kosis_api('20231019')
    #
    engine = db_engine_get()
    #
    # sample_dict = {'rcve_orgn': 'KOSIS',
    #                'idx_id': '100000',
    #                'user_id': '2130049',
    #                'prgm_id': 'python-code',
    #                'idx_nm': 'sample_nm',
    #                'data_lgcf_nm': 'nm',
    #                'data_mdcf_nm': 'test',
    #                'rgn_cls': '안녕',
    #                'idx_peri': 'Q',
    #                'unit': '원',
    #                'rcve_yn': 'Y'
    #                }
    #
    # sample_dict = {'rcve_orgn': 'KOSIS',
    #                'idx_id': '100000',
    #                'idx_pont': '20231023',
    #                'detl_item': 'what',
    #                'user_id': '2130049',
    #                'prgm_id': 'python-code',
    #                'idx_nm': 'sample_nm',
    #                'data_lgcf_nm': 'nm',
    #                'idx_val': 15,
    #                'unit': 'Q',
    #                'rcve_dt': '20231023'
    #                }

    sample_dict = {'list_no': 'KOSIS',
                   'list_nm': '100000',
                   'base_month': '20231023',
                   'finance_cd': 'what',
                   'finance_nm': '2130049',
                   'account_cd': 'python-code',
                   'account_nm': 'sample_nm',
                   'a': 'a',
                   'b': 'b',
                   'c': 'c'
                   }

    update_row(engine,'imd_ia.tbl_iia_iam_fisis', sample_dict)



    #
    # save_idx_info(engine, sample_dict)
    #
    # print(type(engine))


    # save_kosis_api_list()

# 한국은행 array(['M', 'Q', 'S', 'D', 'SM', 'A'], dtype=object)
