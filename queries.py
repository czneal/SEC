# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 14:41:38 2019

@author: Asus
"""

#queries
select_newest_reports = """
select r.adsh, r.cik, r.fin_year, r.file_date, form from reports as r,
(
	select cik, fin_year, max(file_date) as file_date 
	from reports
	group by cik, fin_year
) m
where r.cik = m.cik 
	and r.fin_year = m.fin_year 
    and r.file_date = m.file_date
    and r.fin_year<=%(fy)s;
"""