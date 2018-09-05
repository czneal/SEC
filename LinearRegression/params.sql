select distinct tag from nums n, subs s 
where sic in ("6021","6022","6029","6099","6162") and form in ('10-K', '10-Q')
      and n.adsh = s.adsh
      and ddate like '2016%'
      and qtrs in(1,2,3,4)
      and coreg = ''
      and uom = 'USD'
      and version like 'us-gaap%'
      and flag=1
      and value <> ""
order by tag