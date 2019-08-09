from datetime import date, datetime, time, timedelta


def get_period(start, end):
    """ See doctests for reference.
    
    >>> get_period('2019-01-01', '2019-01-02')
    (datetime.datetime(2019, 1, 1, 0, 0), datetime.datetime(2019, 1, 2, 23, 59, 59, 999999))
    
    """
    date_format = '%Y-%m-%d'
    
    start = datetime.strptime(start, date_format)
    end = datetime.strptime(end, date_format)
    
    def replace_time(dt, new_time):
        return dt.replace(
            hour=new_time.hour,
            minute=new_time.minute,
            second=new_time.second,
            microsecond=new_time.microsecond,
        )

    start = replace_time(start, time.min)
    end = replace_time(end, time.max)
    
    return start, end


def yield_dates(start: date, end: date, as_str=False):
    """Generates inclusive list of dates between start end end.
    """
    days_count = int((end - start).days)
    for i in range(days_count + 1):
        dt = start + timedelta(days=i)
        if as_str:
            dt = str(dt)
        yield dt


if __name__ == "__main__":
    import doctest
    doctest.testmod()
