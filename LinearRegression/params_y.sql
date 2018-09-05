select s.cik, n.value from nums n, subs s 
where sic in ("6021","6022","6029","6099","6162") and form = '10-K'
	and n.adsh = s.adsh
	and ddate like fy+'%'
	and qtrs = ?
	and coreg = ''
	and uom = 'USD'
	and version like 'us-gaap%'
	and tag = ?
order by cik