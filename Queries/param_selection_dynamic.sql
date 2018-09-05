/*
drop temporary table if exists req_tags0;
drop temporary table if exists req_tags1;
create temporary table req_tags1 (tag VARCHAR(256) CHARACTER SET utf8 not null);
create temporary table req_tags0 (tag VARCHAR(256) CHARACTER SET utf8 not null);
insert into req_tags1 (tag) values ("PaymentsToAcquireBusinessesGross");
insert into req_tags1 (tag) values ("PaymentsToAcquireBusinessesNetOfCashAcquired");
insert into req_tags (tag) values ("PaymentsToAcquireBusinessesAndInterestInAffiliates");
insert into req_tags (tag) values ("PaymentsToAcquireSoftware");
insert into req_tags (tag) values ("PaymentsToDevelopSoftware");
insert into req_tags0 (tag) values ("CapitalizedComputerSoftwareNet");
insert into req_tags1 (tag) values ("CapitalizedComputerSoftwareNet");
insert into req_tags1 (tag) values ("mg_payments_capital");
*/
          
select n1.*, n0.v0 from
(
	select n.adsh, n.tag, s.fy, value as v1, uom, s.cik
	from Subs s, Nums n, req_tags1 t, pre p
	where form='10-K'
		and s.adsh = n.adsh
		and fy = %(fy1)s
		and period = ddate
		and n.version like 'us-gaap%'
		and (uom ="USD" or uom = "shares" or uom="pure") 
		and coreg = ""
		and (n.qtrs = 0 or n.qtrs = 4)
        and n.tag = t.tag
		and p.adsh = s.adsh
		and p.tag = n.tag
		and p.stmt <> 'UN'
) n1
left outer join
(
	select n.adsh, n.tag, s.fy, value as v0, uom, s.cik
	from Subs s, Nums n, req_tags0 t, pre p
	where form='10-K'
		and s.adsh = n.adsh
		and fy = %(fy0)s
		and period = ddate
		and n.version like 'us-gaap%'
		and (uom ="USD" or uom = "shares" or uom="pure") 
		and coreg = ""
		and (n.qtrs = 0 or n.qtrs = 4)
        and n.tag = t.tag
		and p.adsh = s.adsh
		and p.tag = n.tag
		and p.stmt <> 'UN'
) n0
on n0.tag = n1.tag
	and n0.cik = n1.cik
order by adsh, n1.tag;