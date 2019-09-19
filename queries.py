# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 14:41:38 2019

@author: Asus
"""

#queries
select_newest_reports = """
select r.adsh, r.cik, r.fin_year as fy, r.file_date, r.form 
from reports as r, 
(
	select cik, fin_year, max(file_date) as file_date, max(adsh) as adsh
	from reports
	group by cik, fin_year
) m
where r.cik = m.cik 
	and r.fin_year = m.fin_year 
    and r.file_date = m.file_date
    and r.fin_year<=%(fy)s
    and r.adsh = m.adsh;    
"""

select_newest_reports_ciks = """
select r.adsh, r.cik, r.fin_year as fy, r.file_date, r.form 
from reports as r, tmp_ciks t,
(
	select cik, fin_year, max(file_date) as file_date, max(adsh) as adsh
	from reports
	group by cik, fin_year
) m
where r.cik = m.cik 
    and r.cik = t.cik
	and r.fin_year = m.fin_year 
    and r.file_date = m.file_date
    and r.fin_year<=%(fy)s
    and r.adsh = m.adsh;
"""

create_tmp_cik_table = """
create temporary table tmp_ciks (cik INT(11) not null, PRIMARY KEY (cik));
"""

insert_tmp_cik = """
insert into tmp_ciks (cik) values (%s);
"""

create_tmp_adsh_table = """
create temporary table tmp_adshs 
(adsh VARCHAR(20) CHARACTER SET utf8 not null, PRIMARY KEY (adsh));
"""

insert_tmp_adsh = """
insert into tmp_adshs (adsh) values (%s);
"""

clear_calc_tables = """
delete from mgnums where tag like 'mg_%';
truncate table mgreporttable;
truncate table mgparamstype1;
"""

clear_calc_tables_adsh = """
delete mgnums 
from mgnums, tmp_adshs
where mgnums.adsh = tmp_adshs.adsh
    and tag like 'mg_%';

delete mgreporttable 
from mgreporttable, tmp_adshs
where mgreporttable.adsh = tmp_adshs.adsh;

delete mgparamstype1 
from mgparamstype1, tmp_adshs
where mgparamstype1.adsh = tmp_adshs.adsh;
"""

insert_update_companies = """
INSERT INTO companies (company_name, cik, sic, updated)
VALUES (%(company_name)s, %(cik)s, %(sic)s, %(updated)s)
ON DUPLICATE KEY UPDATE
company_name = IF(values(updated) >= updated, values(company_name), company_name),
cik = IF(values(updated) >= updated, values(cik), cik),
sic = IF(values(updated) >= updated, values(sic), sic),
updated = IF(values(updated) >= updated, values(updated), updated);
"""

update_nasdaq_changes = """
update nasdaq 
set ticker=%(new_ticker)s
where ticker=%(old_ticker)s;
"""

select_new_companies = """
select distinct f.cik
from sec_forms f 
left outer join companies c
on c.cik = f.cik
where (c.cik is null or 
      (c.updated < f.filed and c.company_name <> f.company_name));
"""