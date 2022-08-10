import datetime

today = datetime.date.today()
day_1 = today - datetime.timedelta(1)

yearmonth = day_1.strftime('%Y%m')
month_name = str(day_1.month) + '월'