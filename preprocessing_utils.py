import re
from datetime import datetime


def map_url_to_id(url):
    """
    https://www.imdb.com/title/tt10627548/ -> tt10627548
    """
    id = None
    try:
        id = url.split('/')[-2]
    except:
        id = None
    finally:
        return id


def map_budget(str_budget):
    """
    '$80,000,000' -> 80000000
    '90. 000. 000 $' -> 90000000
    """
    budget = None
    try:
        budget = int(''.join(re.split(r'\$|,|\s+|\.', str_budget)))
    except:
        budget = None
    finally:
        return budget


def map_str_to_list(my_string):
    """
    'Canada, USA' -> ['Canada', 'USA']
    'Atlanta,    Georgia; Da Nang' -> ['Atlanta', 'Georgia', 'Da Nang']
    """
    my_list = None
    try:
        my_list = re.split(r';|,', my_string)
        my_list = [it.strip() for it in my_list]
    except:
        my_list = None
    finally:
        return my_list


def map_str_to_datetime(s):
    """
    '22 November 2020    ' -> datetime object
    '21 August 2020 (USA)'
    'May 2011 (USA) '
    """
    date_time = None
    try:
        tmp = s.split('(')[0]
        tmp = tmp.strip()
        try:
            date_time = datetime.strptime(tmp, '%d %B %Y')
        except:
            date_time = None
        if date_time is None:
            try:
                date_time = datetime.strptime(tmp, '%B %Y')
            except:
                date_time = None
    except:
        date_time = None
    finally:
        return date_time


def map_runtime(s):
    """
    ' 95 min' -> 95
    """
    runtime = None
    try:
        runtime = int(re.findall(r'\d+', s)[0])
    except:
        runtime = None
    finally:
        return runtime