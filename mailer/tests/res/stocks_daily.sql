/*
-- Query: select * from stocks_daily
where ticker in ('aapl', 'wfc', 'bac', 'jpm')
	and trade_date between '2020-02-01' and '2020-02-10'
order by ticker, trade_date desc
LIMIT 0, 10000

-- Date: 2020-06-05 16:09
*/
delete from `stocks_daily` where ticker in ('aapl', 'wfc', 'bac', 'jpm') and trade_date between '2020-02-01' and '2020-02-10';
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('AAPL','2020-02-10',314.18,321.55,313.85,321.55,27337220);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('AAPL','2020-02-07',322.37,323.40,318.00,320.03,29421010);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('AAPL','2020-02-06',322.57,325.22,320.26,325.21,26356390);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('AAPL','2020-02-05',323.52,324.76,318.95,321.45,29706720);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('AAPL','2020-02-04',315.31,319.64,313.63,318.85,34154130);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('AAPL','2020-02-03',304.30,313.49,302.22,308.66,43496400);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('BAC','2020-02-10',34.44,34.69,34.38,34.69,24373790);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('BAC','2020-02-07',34.32,34.66,34.25,34.61,31263030);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('BAC','2020-02-06',34.94,35.01,34.60,34.67,39511500);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('BAC','2020-02-05',34.10,34.81,34.07,34.71,54604760);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('BAC','2020-02-04',33.56,33.90,33.53,33.62,45061580);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('BAC','2020-02-03',33.00,33.40,32.92,32.97,48508200);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('JPM','2020-02-10',136.95,137.85,136.75,137.74,5960700);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('JPM','2020-02-07',136.73,137.67,136.30,137.17,6379784);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('JPM','2020-02-06',138.24,138.29,137.11,137.61,8992592);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('JPM','2020-02-05',136.57,137.73,136.01,137.59,10150710);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('JPM','2020-02-04',135.55,136.60,135.08,135.29,9157126);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('JPM','2020-02-03',132.66,134.24,132.66,133.37,10021370);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('WFC','2020-02-10',47.67,47.86,47.42,47.77,18124750);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('WFC','2020-02-07',47.73,48.00,47.48,47.84,13174650);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('WFC','2020-02-06',48.44,48.50,47.85,47.98,18259190);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('WFC','2020-02-05',47.90,48.40,47.78,48.31,20141770);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('WFC','2020-02-04',47.70,47.84,47.25,47.26,14973290);
INSERT INTO `stocks_daily` (`ticker`,`trade_date`,`open`,`high`,`low`,`close`,`volume`) VALUES ('WFC','2020-02-03',47.24,47.72,47.03,47.12,15471990);
