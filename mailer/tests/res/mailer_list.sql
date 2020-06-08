/*
-- Query: select * from mailer_list 
where email in ('victor@machinegrading.ee', 'vkugushev@gmail.com')
LIMIT 0, 10000

-- Date: 2020-06-08 15:41
*/
delete from `mailer_list` where email in ('victor@machinegrading.ee', 'vkugushev@gmail.com');
INSERT INTO `mailer_list` (`email`,`subscription`,`metadata`) VALUES ('victor@machinegrading.ee','dividents','[\"wfc\", \"aapl\", \"brk.a\", \"ge\"]');
INSERT INTO `mailer_list` (`email`,`subscription`,`metadata`) VALUES ('victor@machinegrading.ee','insider','{\n    \"2488\": [\n        \"buy\"\n    ],\n    \"40545\": [\n        \"buy\"\n    ],\n    \"70858\": [\n        \"buy\",\n        \"sell\"\n    ]\n}');
INSERT INTO `mailer_list` (`email`,`subscription`,`metadata`) VALUES ('victor@machinegrading.ee','logs','[\"fatal\"]');
INSERT INTO `mailer_list` (`email`,`subscription`,`metadata`) VALUES ('victor@machinegrading.ee','shares','[\n	\"wfc\",\n	\"aapl\",\n	\"brk.a\",\n	\"ge\",\n	\"bac\"\n]');
INSERT INTO `mailer_list` (`email`,`subscription`,`metadata`) VALUES ('victor@machinegrading.ee','stocks','{\n	\"bac\" : [\n		30,\n		32\n	],\n	\"wfc\" : [\n		28,\n		30\n	]\n}');
INSERT INTO `mailer_list` (`email`,`subscription`,`metadata`) VALUES ('vkugushev@gmail.com','dividents','[\"wfc\", \"aapl\", \"brk.a\", \"ge\"]');
INSERT INTO `mailer_list` (`email`,`subscription`,`metadata`) VALUES ('vkugushev@gmail.com','insider','{\n	\"2488\" : [\n		\"sell\"\n	],\n	\"40545\" : [\n		\"buy\"\n	],\n	\"70858\" : [\n		\"buy\",\n		\"sell\"\n	]\n}');
INSERT INTO `mailer_list` (`email`,`subscription`,`metadata`) VALUES ('vkugushev@gmail.com','logs','[\"xbrl\", \"fatal\", \"stocks\"]');
INSERT INTO `mailer_list` (`email`,`subscription`,`metadata`) VALUES ('vkugushev@gmail.com','reports','{\n	\"2488\" : [\n		\"y\",\n		\"q\"\n	],\n	\"40545\" : [\n		\"q\"\n	],\n	\"70858\" : [\n		\"y\"\n	]\n, "40730": ["y"]}');
INSERT INTO `mailer_list` (`email`,`subscription`,`metadata`) VALUES ('vkugushev@gmail.com','shares','[\n	\"wfc\",\n	\"aapl\",\n	\"brk.a\",\n	\"ge\",\n	\"bac\"\n]');
INSERT INTO `mailer_list` (`email`,`subscription`,`metadata`) VALUES ('vkugushev@gmail.com','stocks','{\n	\"bac\" : [\n		30,\n		32\n	],\n	\"wfc\" : [\n		28,\n		30\n	]\n}');
