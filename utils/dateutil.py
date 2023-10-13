import datetime

def quarter_get(date_value):
    """
    입력된 날짜의 직전 분기 YYYYMM 형태로 구하는 함수
    :param date_value:
    :return:
    """
    current_month = date_value.month
    current_year = date_value.year

    previous_quarter_month = 0
    previous_quarter_year = 0

    if current_month in [1, 2, 3]:
        previous_quarter_month = 12
    elif current_month in [4, 5, 6]:
        previous_quarter_month = 3
    elif current_month in [7, 8, 9]:
        previous_quarter_month = 6
    elif current_month in [10, 11, 12]:
        previous_quarter_month = 9

    if current_month in [1, 2, 3]:
        previous_quarter_year = current_year - 1
    else:
        previous_quarter_year = current_year

    quarter_date = datetime.datetime(previous_quarter_year, previous_quarter_month, 1)
    quarter = quarter_date.strftime('%Y%m')

    return quarter