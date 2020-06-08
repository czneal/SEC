/*
-- Query: select * from stocks_shares
where ticker in ('aapl', 'wfc', 'bac', 'jpm')
	and trade_date between '2020-02-01' and '2020-04-01'
order by ticker, trade_date desc
LIMIT 0, 10000

-- Date: 2020-06-05 16:08
*/
delete from `stocks_shares` where ticker in ('aapl', 'wfc', 'bac', 'jpm') and trade_date between '2020-02-01' and '2020-04-01';
INSERT INTO `stocks_shares` (`ticker`,`trade_date`,`market_cap`,`shares`) VALUES ('AAPL','2020-02-03',1350535656800,4375480000);
INSERT INTO `stocks_shares` (`ticker`,`trade_date`,`market_cap`,`shares`) VALUES ('BAC','2020-03-11',197790564147,8724771246);
INSERT INTO `stocks_shares` (`ticker`,`trade_date`,`market_cap`,`shares`) VALUES ('BAC','2020-02-24',285684796981,8728530308);
INSERT INTO `stocks_shares` (`ticker`,`trade_date`,`market_cap`,`shares`) VALUES ('JPM','2020-02-28',356919424884,3073976616);
INSERT INTO `stocks_shares` (`ticker`,`trade_date`,`market_cap`,`shares`) VALUES ('WFC','2020-03-19',115689069788,4089398013);
INSERT INTO `stocks_shares` (`ticker`,`trade_date`,`market_cap`,`shares`) VALUES ('WFC','2020-03-02',173261234171,4099887226);
