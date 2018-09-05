select n.adsh, n.tag, s.fy, value, uom 
from Subs s, Nums n, pre p, tags t, req_tags tt
where form='10-K'
	and s.adsh = n.adsh
	and fy = %(fy)s
	and period = ddate
	and n.version like 'us-gaap%'
	and (uom =%(uom)s) 
	and coreg = ""
	and (n.qtrs = 0 or n.qtrs = 4) 
    and p.adsh = s.adsh
    and p.tag = n.tag
    and p.stmt <> 'UN'
    and t.tag = n.tag 
    and t.version = n.version
    and t.datatype = 'monetary'
    and tt.tag = n.tag
order by adsh, n.tag