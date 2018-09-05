
/*
drop temporary table if exists req_tags;
drop temporary table if exists req_mg_tags;
create temporary table req_tags (tag VARCHAR(256) CHARACTER SET utf8 not null);
create temporary table req_mg_tags (tag VARCHAR(256) CHARACTER SET utf8 not null);
insert into req_tags (tag) values ("PaymentsToAcquireBusinessesGross");
insert into req_tags (tag) values ("PaymentsToAcquireBusinessesNetOfCashAcquired");
insert into req_tags (tag) values ("PaymentsToAcquireBusinessesAndInterestInAffiliates");
insert into req_tags (tag) values ("PaymentsToAcquireSoftware");
insert into req_tags (tag) values ("PaymentsToDevelopSoftware");
insert into req_mg_tags (tag) values ("PaymentsForSoftware");
insert into req_mg_tags (tag) values ("mg_payments_capital");
*/


select n.adsh, n.tag, s.fy, value, uom 
from Subs s, Nums n, req_tags t, pre p
where form='10-K'
	and s.adsh = n.adsh
	and fy = %(fy)s
	and period = ddate
	and n.version like 'us-gaap%'
	and (uom ="USD" or uom = "shares" or uom="pure") 
	and coreg = ""
	and t.tag = n.tag
	and (n.qtrs = 0 or n.qtrs = 4) 
    and p.adsh = s.adsh
    and p.tag = n.tag
    and p.stmt <> 'UN'
union
select mgn.* from mgnums mgn, req_mg_tags t
where fy = %(fy)s
	and t.tag = mgn.tag
order by adsh, tag;
