select tg.*, n.sign from taggraph tg
left outer join
( 
	select r.cik, concat(version,':',tag) as tagname, fin_year,
		case value>=0
			when true then 1
            else -1
		end as sign
	from reports r, mgnums n
	where n.adsh = r.adsh
		and fin_year in (2013,2014,2015,2016,2017)
        and form='10-k'
) n
on tg.child = tagname
	and tg.cik = n.cik
    and tg.fy = n.fin_year limit 10000