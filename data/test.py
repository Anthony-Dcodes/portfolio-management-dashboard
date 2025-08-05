from datetime import datetime as dt
from datetime import timedelta

import pandas as pd

date = "20250805"
datetime_date = dt.strptime(date, "%Y%m%d")
print(datetime_date)
datetime_date = datetime_date + timedelta(days=1)
print(datetime_date)
datetime_date_converted = datetime_date.strftime("%Y%m%d")
print(datetime_date_converted)
