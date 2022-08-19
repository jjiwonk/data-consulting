import datetime

today = datetime.date.today()
day_1 = datetime.date(year=today.year, month=today.month, day=1)
day_1 = day_1 - datetime.timedelta(1)
start_day = datetime.date(year=day_1.year, month=day_1.month, day=1)
yearmonth = day_1.strftime('%Y%m')
month_name = str(day_1.month) + '월'