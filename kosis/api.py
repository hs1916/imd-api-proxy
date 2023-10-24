import requests
import pandas as pd
from utils.dateutil import seperator_period

DEFAULT_QRY_PARAM = "method=getList&service=4&serviceDetail=indIdDetail&format=json&jsonVD=Y&rn=0&srvRn=1&pageNo=1&numOfRows=1000"
KOSIS_URL = "https://kosis.kr/openapi/indIdDetailSearchRequest.do";


def send_kosis_api(prd_de: str, jipyo_id: str, api_key: str):
    """
    통계청 api 호출
    :param prd_de: 데이터 주기
    :param jipyo_id: 지표ID
    :param api_key: 통계청 api key
    :return:
    """
    api_url = f"{KOSIS_URL}?{DEFAULT_QRY_PARAM}&apiKey={api_key}&startPrdDe={prd_de}&endPrdDe={prd_de}&jipyoId={jipyo_id}"
    response = object()
    try:
        response = requests.get(api_url)
    except:
        print(f'[KOSIS] statistic_info get api error... {jipyo_id}/{prd_de}')

    return response
