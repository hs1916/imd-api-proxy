import requests
from bs4 import BeautifulSoup
import pandas as pd

gen_req_url = 'https://www.kofiabond.or.kr/proframeWeb/XMLSERVICES/'
fund_gen_url = 'https://dis.kofia.or.kr/proframeWeb/XMLSERVICES/'


def send_std_yield(base_date: str):
    # 채권시가 조회 XML
    form_data_bond = f'''
                    <message>
                        <proframeHeader>
                        <pfmAppName>BIS-KOFIABOND</pfmAppName>
                        <pfmSvcName>BISBndSrtPrcSrchSO</pfmSvcName>
                        <pfmFnName>selectDay</pfmFnName>
                        </proframeHeader>
                        <systemHeader></systemHeader>
                    <BISBndSrtPrcDayDTO><standardDt>{base_date}</standardDt><reportCompCd></reportCompCd><applyGbCd>C00</applyGbCd></BISBndSrtPrcDayDTO></message>
                    '''

    # service 호출 및 xml 파싱
    r = requests.post(gen_req_url, form_data_bond)
    soup = BeautifulSoup(r.content, 'lxml-xml')

    # xml 컬럼 정보
    col_val_dt = {
        'val1': '3월',
        'val2': '6월',
        'val3': '9월',
        'val4': '1년',
        'val5': '1년6월',
        'val6': '2년',
        'val7': '2년6월',
        'val8': '3년',
        'val9': '4년',
        'val10': '5년',
        'val11': '7년',
        'val12': '10년',
        'val13': '15년',
        'val14': '20년',
        'val15': '30년',
        'val16': '50년'
    }

    # response 모든 row Get
    res_li = soup.find_all('BISBndSrtPrcDayDTO')

    df = pd.DataFrame()
    for row in res_li:
        dt = dict()
        data_list = list()
        bond_list = list()
        bond_detail = list()
        credit_grade = list()
        val_list = list()
        grade_list = list()

        for key, value in col_val_dt.items():
            bond_list.append(row.find('largeCategoryMrk').text)  # 채권종류
            bond_detail.append(row.find('typeNmMrk').text)  # 채권 세부
            credit_grade.append(row.find('creditRnkMrk').text)  # 신용등급
            grade_list.append(row.find('koreanShotNm').text)  # 평가사
            val_list.append(value)
            data_list.append(row.find(key).text)

        df_sub = pd.DataFrame({
            '채권분류': bond_list,
            '채권유형': bond_detail,
            '신용등급': credit_grade,
            '평가사': grade_list,
            '기간': val_list,
            '호가': data_list
        })

        df = pd.concat([df, df_sub])

    return df.to_dict(orient='records')


def send_call_yield(base_date: str):
    # 채권 호가 수익률
    form_data_bond = f'''
                        <message>
                        <proframeHeader>
                            <pfmAppName>BIS-KOFIABOND</pfmAppName>
                            <pfmSvcName>BISLastAskPrcROPSrchSO</pfmSvcName>
                            <pfmFnName>listDay</pfmFnName>
                        </proframeHeader>
                        <systemHeader></systemHeader>
                        <BISComDspDatDTO><val1>{base_date}</val1></BISComDspDatDTO></message>
                    '''

    # service 호출 및 xml 파싱
    r = requests.post(gen_req_url, form_data_bond)
    soup = BeautifulSoup(r.content, 'lxml-xml')

    col_dt = {
        'val1': '채권명',
        'val2': '채권기간',
        'val3': '오전',
        'val4': '오후',
        'val5': '전일대비',
        'val6': '전일',
        'val7': '연중최고',
        'val8': '연중최저'
    }

    res_li = soup.find_all('BISComDspDatDTO')

    df = pd.DataFrame()
    for r in res_li:
        dt = dict()
        for i in list(col_dt.keys()):
            dt[i] = [r.find(i).text]
        df_sub = pd.DataFrame.from_dict(dt)
        df = pd.concat([df, df_sub])

    df.reset_index(drop=True, inplace=True)

    return df.to_dict(orient='records')


def send_fund_std_val(base_date: str):
    """
    금융투자협회 펀드기준가 크롤링
    :param base_date: 기준일자
    :return: 펀드기준가
    """
    # 펀드기준가
    form_data_bond = f'''
                        <message>
                            <proframeHeader>
                                <pfmAppName>FS-DIS2</pfmAppName>
                                <pfmSvcName>DISFundStdPriceSO</pfmSvcName>
                                <pfmFnName>select</pfmFnName>
                            </proframeHeader>
                            <systemHeader></systemHeader>
                            <DISCondFuncDTO>
                                <tmpV30>{base_date}</tmpV30>
                                <tmpV3></tmpV3>
                                <tmpV4></tmpV4>
                                <tmpV7></tmpV7>
                                <tmpV5></tmpV5>
                                <tmpV11></tmpV11>
                                <tmpV12></tmpV12>
                                <tmpV50></tmpV50>
                                <tmpV51></tmpV51>
                            </DISCondFuncDTO>
                        </message>
                    '''
    # service 호출 및 xml 파싱
    r = requests.post(fund_gen_url, form_data_bond)
    soup = BeautifulSoup(r.content, 'lxml-xml')

    # xml 컬럼 별 데이터 매핑
    col_dt = {
        'tmpV1': '운용사',
        'tmpV2': '펀드명',
        'tmpV3': '펀드형태',
        'tmpV4': '설정일',
        'tmpV5': '설정원본',
        'tmpV6': '기준가격(가격)',
        'tmpV7': '기준가격(과표)',
        'tmpV8': '자산구성(주식)',
        'tmpV9': '자산구성(채권)',
        'tmpV10': '자산구성(유동)',
        'tmpV12': '표준코드'
    }
    res_li = soup.find_all('selectMeta')

    # 수신 데이터 datframe 으로 전환
    df = pd.DataFrame()
    for r in res_li:
        dt = dict()
        for i in list(col_dt.keys()):
            dt[i] = [r.find(i).text]
        df_sub = pd.DataFrame.from_dict(dt)
        df = pd.concat([df, df_sub])

    df.reset_index(drop=True, inplace=True)
    return df.to_dict(orient='records')


if __name__ == '__main__':
    pass
