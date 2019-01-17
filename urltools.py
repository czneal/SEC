# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 13:28:01 2018

@author: Asus
"""
import sys
import urllib
import io

def fetch_urlfile(url_text, filename = None, log=None, tryout = 3):
    """
    download file from url_text to filename if not None or return BytesIO object
    return True or False if succeded when filename declared
    return BytesIO object or None when filename is None

    log - LogFile object if None works silently
    tryout - count of tryouts when download file
    """

    good_read = False
    body = None

    while tryout>0:
        try:
            req = urllib.request.Request(url_text)
            url = urllib.request.urlopen(req)
            body = url.read()
            url.close()
            good_read = True
            break
        except:
            if log:
                log.write("%d attempt of downloading %s fail" %(4-tryout, url_text))
                log.write_tb(sys.exc_info())
            tryout = tryout - 1
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
        except:
            return ""
        return filename

    return io.BytesIO(body)