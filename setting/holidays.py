import requests
import datetime
# 참고: https://iamaman.tistory.com/3647

def get_holidays(from_date, to_date):
    from_year = from_date.year
    to_year = to_date.year
    def get_dates(year):
        url = "https://open.krx.co.kr/contents/OPN/99/OPN99000001.jspx"
        data = {"search_bas_yy": year,"gridTp": "KRX", "pagePath": "/contents/MKD/01/0110/01100305/MKD01100305.jsp",
        "code": 'VwN0qWxNxoQd3GptLiFi7VpQSV4Ewa+d2Su7DXPyhf9QzGrcwc/rwEcTS38k4e2df5Yx0Mfnbi2PWDHmer4lQzKMoOk5t9O8/DabZgelyz9UBc82a6GP7G4MABRDdIaJ7T+v79W6ON5hsRRGRUrUj69+eqY/BlbgIhBGzjGwqsT+CtNJN0ckkY/7efqYEaL7',
        'pageFirstCall': 'Y'}
        content_type = 'application/x-www-form-urlencoded; charset=UTF-8'
        response = requests.post(url=url, data=data, headers={'Content-Type': content_type})
        resultJson = response.json()
        holidays = [x['calnd_dd_dy'] for x in resultJson['block1']]
        return holidays

    holiday_list = get_dates(from_year)
    holiday_list = list(dict.fromkeys(holiday_list + get_dates(to_year)))

    return holiday_list
