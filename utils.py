# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 17:58:11 2019

@author: Asus
"""

import datetime as dt
import calendar
import re
import os
import pathlib
import lxml.etree  # type: ignore

from typing import Tuple, Optional, cast, Match, Union, IO, Callable, Any, Type

from settings import Settings


class ProgressBar(object):
    def __init__(self):
        self.total: int = 0
        self.one_step: dt.timedelta = dt.timedelta()
        self.n: int = 0
        self.before: dt.datetime = dt.datetime.now()

    def start(self, total: int = -1) -> None:
        ProgressBar.__init__(self)
        self.total = total

    def measure(self) -> None:
        delta = dt.datetime.now() - self.before
        self.before = dt.datetime.now()
        self.one_step = (self.one_step * self.n + delta) / (self.n + 1)
        self.n += 1

    def message(self) -> str:
        if self.total == -1:
            s2 = '?'
        else:
            s2 = str(self.one_step * (self.total - self.n)).split('.')[0]
        s3 = str(self.one_step)
        return 'processed {0} of {1}, time remains: {2}, time per step: {3}'.format(
            self.n, '?' if self.total == -1 else self.total, s2, s3)


def correct_date(y: int, m: int, d: int) -> dt.date:
    (_, last) = calendar.monthrange(y, m)
    if d > last:
        d = last
    return dt.date(y, m, d)


def periodend(fy: int, m: int, d: int) -> dt.date:
    if m <= 6:
        return correct_date(fy + 1, m, d)
    else:
        return correct_date(fy, m, d)


def calculate_fy_fye(period: dt.date) -> Tuple[int, str]:
    fye = str(period.month).zfill(2) + str(period.day).zfill(2)
    if period.month > 6:
        fy = period.year
    else:
        fy = period.year - 1

    return(fy, fye)


def str2date(datestr: Optional[Union[str,
                                     dt.date,
                                     dt.datetime]],
             pattern: str = 'ymd') -> Optional[dt.date]:
    if isinstance(datestr, dt.date):
        return cast(dt.date, datestr)
    if datestr is None:
        return None

    assert isinstance(datestr, str)
    patterns = [re.compile('.*\d{4}-\d{2}-\d{2}.*'),
                re.compile(r'.*\d{4}/\d{2}/\d{2}.*'),
                re.compile(r'.*\d{8}.*'),
                re.compile('.*\d{2}/\d{2}/\d{4}.*')]
    assert sum([p.match(datestr) is not None for p in patterns]) > 0
    assert pattern in {'ymd', 'mdy'}

    retval: Optional[dt.date] = None
    try:
        for p in patterns:
            dd = p.search(datestr)
            if dd is not None:
                break
        dd = cast(Match[str], dd)
        datestr = dd.group(0).replace('-', '').replace('/', '')
        if pattern == 'ymd':
            retval = dt.date(int(datestr[0:4]),
                             int(datestr[4:6]),
                             int(datestr[6:8]))
        if pattern == 'mdy':
            retval = dt.date(int(datestr[4:8]),
                             int(datestr[0:2]),
                             int(datestr[2:4]))
    except BaseException:
        pass

    return retval


def opensmallxmlfile(file: Optional[IO]) -> Optional[lxml.etree.Element]:
    if file is None:
        return None

    root = None
    try:
        root = lxml.etree.parse(file).getroot()
    except BaseException:
        file.close()

    return root


def openbigxmlfile(file: Optional[IO]) -> Optional[lxml.etree.Element]:
    if file is None:
        return None

    root = None
    try:
        #xmlparser = etree.XMLParser(recover=True)
        xmlparser = lxml.etree.XMLParser(huge_tree=True)
        tree = lxml.etree.parse(file, parser=xmlparser)
        root = tree.getroot()
    except BaseException:
        file.close()

    return root


def class_for_name(module_name: str, class_name: str) -> Any:
    # load the module, will raise ImportError if module cannot be loaded
    m = __import__(module_name, globals(), locals(), class_name)
    # get the class, will raise AttributeError if class cannot be found
    c = getattr(m, class_name)
    return c


def retry(retry: int, exc_cls: Type[Exception]) -> Any:
    def decorator(function):
        def wrapper(*args, **kwargs):
            for i in range(retry):
                try:
                    return function(*args, **kwargs)
                except exc_cls as e:
                    if i + 1 == retry:
                        print('done {} tryouts'.format(retry))
                        raise e
        return wrapper
    return decorator


def clear_dir(target_dir):
    for [root, dirs, filenames] in os.walk(target_dir):
        for filename in filenames:
            os.remove(os.path.join(root, filename))


def remove_common_path(common: str, path: str) -> str:
    path = os.path.normcase(os.path.normpath(path))
    try:
        common = os.path.commonpath([common, path])
        common = os.path.normcase(os.path.normpath(common) + '/')
        path = path.replace(common, '')
    except ValueError:
        pass

    return pathlib.PurePath(path).as_posix()


def remove_root_dir(path: str) -> str:
    return remove_common_path(Settings.root_dir(), path)


def remove_app_dir(path: str) -> str:
    return remove_common_path(Settings.app_dir(), path)


def add_root_dir(path: str) -> str:
    root_dir = os.path.normcase(Settings.root_dir())
    path = os.path.normcase(path)

    return pathlib.PurePath(os.path.join(root_dir, path)).as_posix()


def add_app_dir(path: str) -> str:
    app_dir = os.path.normcase(Settings.app_dir())
    path = os.path.normcase(path)

    return pathlib.PurePath(
        os.path.join(app_dir, path)).as_posix()


def posix_join(path: str, *paths: str) -> str:
    ret_path = os.path.join(path, *paths)
    return pathlib.PurePath(ret_path).as_posix()


def year_month_dir(year: int, month: int) -> str:
    """
    return full path to current month and year
    root_dir/year/month
    """
    path = '{0}/{1}/'.format(str(year), str(month).zfill(2))
    return add_root_dir(path)


if __name__ == '__main__':
    print(posix_join('c:\\', 'docs', 'utils'))
