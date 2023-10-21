import os
import time

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm
from datetime import datetime
import requests


load_dotenv()
kosis_api_key = os.getenv('kosis_api_key')
DEFAULT_QRY_PARAM = "method=getList&service=4&serviceDetail=indIdDetail&format=json&jsonVD=Y&rn=0&srvRn=1&pageNo=1&numOfRows=1000"
KOSIS_URL = "https://kosis.kr/openapi/indIdDetailSearchRequest.do";


def seperator_period(period, date_value):
    result = str()

    if 'Y' in period:
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

def assemble_api_url():
    kosis_df = pd.read_excel('./kosis_list.xlsx')

    api_data = pd.DataFrame()

    with tqdm(total=len(kosis_df[811:815])) as progress_bar:
        for index, row in kosis_df[811:815].iterrows():
            prdDe = seperator_period(row['idx_peri'],datetime.now().strftime('%Y%m%d'))

            api_url = f"{KOSIS_URL}?{DEFAULT_QRY_PARAM}&apiKey={kosis_api_key}&startPrdDe={prdDe}&endPrdDe={prdDe}&jipyoId={row['idx_id']}"

            response = requests.get(api_url)

            if 'err' not in response.json():
                temp_df = pd.DataFrame(response.json())
                api_data = pd.concat([api_data, temp_df])

            time.sleep(0.33)
            progress_bar.update(1)

    api_data.to_excel('./kosis_api_data.xlsx')

if __name__ == '__main__':
    assemble_api_url()