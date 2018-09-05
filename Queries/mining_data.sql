/*
drop temporary table if exists req_tags;

create temporary table req_tags (tag VARCHAR(256) CHARACTER SET utf8 not null);

insert into req_tags (tag) values ("PaymentsToAcquireBusinessesGross");
insert into req_tags (tag) values ("PaymentsToAcquireBusinessesNetOfCashAcquired");
insert into req_tags (tag) values ("PaymentsToAcquireBusinessesAndInterestInAffiliates");
insert into req_tags (tag) values ("PaymentsToAcquireSoftware");
insert into req_tags (tag) values ("PaymentsToDevelopSoftware");

*/

select n.adsh, n.tag, n.fy, value, uom
from mgnums n, req_tags t
where fy = %(fy)s
	and (version = 'us-gaap' or version = 'mg')
	and t.tag = n.tag
order by adsh, n.tag
