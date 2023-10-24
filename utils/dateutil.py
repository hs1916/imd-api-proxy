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
