"""
Created on Tue Sep  5 16:54:20 2017

@author: HeyDude
"""


from datetime import datetime

# =============================================================================
# Set of functions to format (dicom) date time strings
# =============================================================================
DATE_TIME_FORMATS = ('%Y%m%d%H%M%S', '%Y%m%d%H%M%S.%f',
                     '%Y%m%d%H%M%S%z', '%Y%m%d%H%M%S.%f%z')
DATE_FORMATS      = ('%Y%m%d',)
TIME_FORMATS      = ('%H%M%S.%f', '%H%M%S')


def format_time(time='', formats=None):
    """ Parse a specific time string to a datetime object """
    if time == '':
        return time

    if formats is None:
        formats = TIME_FORMATS

    return format_datetime(date_time=time, formats=formats).time()

def format_date(date = '', formats=None):
    """ Parse a specific date string to a datetime object """
    if date == '':
        return date

    if formats is None:
        formats = DATE_FORMATS
    return format_datetime(date_time=date, formats=formats).date()

def format_datetime(date_time='', formats=None):
    """ Parse a specific date and time string to a datetime object """

    if date_time == '':
        return date_time

    if date_time.replace('', ' ') == '':
        return datetime.today()

    if formats is None:
        formats = DATE_TIME_FORMATS
    parsed = None
    for formatting in formats:
        try:
            parsed = datetime.strptime(date_time, formatting)
        except ValueError:
            pass

    if parsed is None:
        parsed = date_time
        print('Unknown Format for date time string: {0}'.format(date_time))
        # raise ValueError
    return parsed
