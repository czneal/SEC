# -*- coding: utf-8 -*-
"""
Created on Tue Dec 18 13:28:01 2018

@author: Asus
"""
import urllib
import sys

def DownloadFile(url_text, filename, log):
    tryout = 3
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
            log.write("\t%d attempt of downloading %s fail" %(4-tryout, url_text))
            log.write(sys.exc_info())
            tryout = tryout - 1
    if not good_read:
       log.write("Couldn't download file %s" % url_text)
    else:
        of = open(filename, "wb")
        of.write(body)
        of.close()

    return good_read