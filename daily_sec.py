# -*- coding: utf-8 -*-
"""
Created on Mon Aug 19 15:09:02 2019

@author: Asus
"""
import datetime
import os

import sec_parse
import glue
import logs
import mysqlio.basicio


def main() -> None:
    # mysqlio.basicio.activate_test_mode()

    logs.configure('mysql', level=logs.logging.INFO)

    d = datetime.datetime.now()
    if d.day == 1 and d.hour == 1:
        d -= datetime.timedelta(days=1)

    glue.update_sec_forms(years=[d.year], months=[d.month])
    glue.update_xbrl_sec_forms(years=[d.year], months=[d.month])
    glue.update_companies_nasdaq()

    glue.download_report_files(method='new', after=datetime.date(2013, 1, 1))
    sec_parse.parse(method='new', after=datetime.date(2013, 1, 1))
    glue.attach_sec_shares_ticker()


if __name__ == '__main__':
    main()
