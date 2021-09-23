import time
import datetime
import pandas as pd


def stamp_dateint(timestamp:int):
    time_local = time.localtime(timestamp)
    dt = time.strftime("%Y%m%d", time_local)
    dt_int = int(dt)
    return dt_int


def dateint_timestamp(date: int):
    """
    :param date: int
    :return: time stamp int 类似1606798800
    """
    date1 = str(date)
    date2 = time.strptime(date1, "%Y%m%d")
    a = time.mktime(date2)
    time1 = str(a)[:-2]
    return time1


def get_valid_dates(conn, security_symbol=None, interval="day", date_from=20210101, date_until=20210201):
    """
    :param conn: pg.connect()
    :param security_symbol: str
    :param interval: day or min
    :param date_from: int 20210101
    :param date_until: int 20210201
    :return: int
    [20210103, 20210104, 20210105, 20210106, 20210107]
    """
    res = list()
    date_from_stamp = dateint_stamp(date_from, "begin")
    date_until_stamp = dateint_stamp(date_until, "end")
    if security_symbol is not None:
        sql = f"select distinct dt from stock_candles_{interval} "\
            f"where symbol='{security_symbol}' "\
            f"and t>={date_from_stamp} and t<={date_until_stamp} "
    date_df = pd.read_sql(sql, conn)
    date_df['date'] = date_df['dt'].dt.strftime('%Y%m%d')
    date_df['date_int'] = date_df['date'].apply(lambda x: int(x))
    result = list(date_df['date_int'])
    return result



def dateint_stamp(date_int: int, style="begin"):
    if style=="begin":
        date1 = str(date_int)+" 00:00:01"
    if style=="end":
        date1 = str(date_int)+" 23:59:59"
    date2 = time.strptime(date1, "%Y%m%d %H:%M:%S")
    a = time.mktime(date2)
    timestamp = str(a)[:-2]
    return timestamp


def add_dateint(date_int: int, add_num: int = 1):
    """
    :param date_int: int
    :param add_num: int
    :return: int
    """
    a = str(date_int)
    b = datetime.date(int(a[0:4]), int(a[4:6]), int(a[-2:]))
    c = b + datetime.timedelta(add_num)
    c_year = str(c.year)
    if c.month < 10:
        c_month = "0"+str(c.month)
    else:
        c_month = str(c.month)
    if c.day < 10:
        c_day = "0"+str(c.day)
    else:
        c_day = str(c.day)
    d = c_year + c_month + c_day
    return d

def datestr_stamp(date_str):
    re_text = r'(?P<day>\d{2})\/(?P<month>\d{2})\/(?P<year>\d{4}):(?P<hms>\S+)\s'
    pattern = re.compile(re_text)
    matches = pattern.finditer(date_str)
    for match in matches:
        day = match.group("day")
        month = match.group("month")
        year = match.group("year")
        hms = match.group("hms")
        date_str = year+month+day+" "+hms
        date = time.strptime(date_str, "%Y%m%d %H:%M:%S")
        stamp = int(str(time.mktime(date))[:-2])
    return stamp