select * from
(select c.*, n.*  from reports r, mgnums n, companies c 
where r.adsh = n.adsh
	and c.cik = r.cik
    and n.tag = 'assets'
    and n.value > 500000000 and n.value<1000000000
    and fin_year = 2017
) q
left outer join nasdaqtraded t
on t.cik = q.cik
order by value desc

select * from nasdaqtraded where symbol in ('jef','','','')

select * from nasdaqtraded where nasdaq_symbol like '%crm%'

select * from nasdaqtraded where security_name like '%american%express%'

select * from companies where company_name like '%american%express%'

select * from companies where cik in (1618755,1618756)