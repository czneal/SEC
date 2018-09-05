create temporary table req_tags (tag VARCHAR(256) CHARACTER SET utf8 not null);
insert into req_tags (tag) values ("PaymentsToAcquireBusinessesGross");
insert into req_tags (tag) values ("PaymentsToAcquireBusinessesNetOfCashAcquired");
insert into req_tags (tag) values ("PaymentsToAcquireBusinessesAndInterestInAffiliates");
insert into req_tags (tag) values ("PaymentsToAcquireSoftware");
insert into req_tags (tag) values ("PaymentsToDevelopSoftware");
insert into req_tags (tag) values ("CapitalizedComputerSoftwareNet");
insert into req_tags (tag) values ("PaymentsForSoftware");
insert into req_tags (tag) values ("mg_payments_capital");

select n.* from
(
	select n.adsh, tag, s.fy, value, uom 
	from Subs s, Nums n
	where form='10-K'
		and s.adsh = n.adsh
		and fy = %(fy)s
		and period = ddate
		and version like 'us-gaap%'
		and (uom ="USD" or uom = "shares" or uom="pure") 
		and coreg = ""
	union
	select * from mgnums
	where fy = %(fy)s
) n, req_tags t
where n.tag = t.tag
order by adsh, n.tag;

select * from nums n