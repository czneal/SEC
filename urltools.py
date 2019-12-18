# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 13:28:01 2018

@author: Asus
"""
import sys
import urllib3  # type: ignore
import certifi
import io
import time

from typing import Optional, cast


def fetch_urlfile(url_text, filename=None, log=None, tryout=3):
    """
    download file from url_text to filename if not None or return BytesIO object
    return True or False if succeded when filename declared
    return BytesIO object or None when filename is None

    log - LogFile object if None works silently
    tryout - count of tryouts when download file
    """

    good_read = False
    body = None

    while tryout > 0:
        try:
            user_agent = {
                'user-agent': 'Mozilla/5.0 (Windows NT 6.3; rv:36.0) ..'}
            http = urllib3.PoolManager(10,
                                       cert_reqs='CERT_REQUIRED',
                                       ca_certs=certifi.where(),
                                       headers=user_agent)
            req = http.request('GET', url_text)
            body = req.data
            good_read = True
            break
        except BaseException:
            if log:
                log.write(
                    "%d attempt of downloading %s fail" %
                    (4 - tryout, url_text))
                log.write_tb(sys.exc_info())
            tryout = tryout - 1
        finally:
            http.clear()

    if not good_read:
        if log:
            log.write("couldn't download file %s" % url_text)
        if filename is None:
            return None
        else:
            return ""

    if filename is not None:
        try:
            with open(filename, 'wb') as f:
                f.write(body)
        except BaseException:
            return ""
        return filename

    return io.BytesIO(body)


def fetch_with_delay(url, tryouts=5) -> Optional[bytes]:
    for tryout in range(tryouts):
        try:
            user_agent = {
                'user-agent': 'Mozilla/5.0 (Windows NT 6.3; rv:36.0) ..'}
            http = urllib3.PoolManager(10,
                                       cert_reqs='CERT_REQUIRED',
                                       ca_certs=certifi.where(),
                                       headers=user_agent)
            req = http.request('GET', url)
            body = req.data

            if body is not None:
                return cast(bytes, body)

        finally:
            time.sleep((tryout) * 3)
            http.clear()

    return None
