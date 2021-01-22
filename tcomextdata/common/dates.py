from calendar import monthrange
from datetime import datetime, date, timedelta
from typing import Tuple

FILENAME_DATE_FORMAT = '%Y%m%d'
DEFAULT_DATE_FORMAT = '%Y-%m-%d'
DEFAULT_MONTH_FORMAT = '%Y-%m'


class PreviousPeriod:
    previous_day = 'day'
    previous_month = 'month'
    previous_quarter = 'quarter'
    previous_half_year = 'halfyear'
    previous_year = 'year'

    @classmethod
    def date_for_filename(cls, from_date: date, period: str,
                          date_format=FILENAME_DATE_FORMAT):
        # only for prev day and month
        if period == cls.previous_day:
            result = from_date - timedelta(days=1)
        else:
            result = from_date.replace(day=1)
        return result.strftime(date_format)


def today() -> str:
    return datetime.today().strftime(FILENAME_DATE_FORMAT)


def prevday(delta: int) -> date:
    return datetime.today() - timedelta(days=delta)


def month_as_dates_range(month, frmt='%Y-%m-%d'):
    start_date = datetime.strptime(month, '%Y%m')
    days = list(monthrange(start_date.year, start_date.month)).pop()
    end_date = start_date.replace(day=days)
    return start_date.strftime(frmt), end_date.strftime(frmt)
