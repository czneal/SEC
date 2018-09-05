select tag, count(tag) as cnt /*n.adsh, n.tag, s.fy, value, uom */
from mgnums n
where fy = %(fy)s
	and version like 'us-gaap'
	and uom = %(uom)s
group by tag