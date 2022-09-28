CREATE TABLE `base_4`  (
  `id` int NOT NULL,
  `col_1` bigint NOT NULL DEFAULT 11111111111111111,
  `col_2` binary NOT NULL DEFAULT b'10101',
  `col_3` bit NOT NULL DEFAULT b'1',
  `col_4` blob,
  `col_5` char NOT NULL DEFAULT 'c',
  `col_6` date NOT NULL DEFAULT '1998-07-14',
  `col_7` datetime NOT NULL DEFAULT '2017-03-02 15:22:22',
  `col_8` decimal(10, 2) NOT NULL DEFAULT '3.80',
  `col_9` double NOT NULL DEFAULT '32.1',
  `col_10` enum('春','夏','秋','冬') NOT NULL DEFAULT '秋',
  `col_11` float NOT NULL DEFAULT '321.0',
  `col_14` int NOT NULL DEFAULT 14,
  `col_15` integer NOT NULL DEFAULT 15,
  `col_16` json,
  `col_18` longblob,
  `col_19` longtext ,
  `col_20` mediumblob,
  `col_21` mediumint NOT NULL DEFAULT 21,
  `col_22` mediumtext,
  `col_26` numeric NOT NULL DEFAULT 123456.1234,
  `col_29` real NOT NULL DEFAULT 123456.1234,
  `col_30` set('fisrt','second','third','fourth','fifth') NOT NULL DEFAULT ('fifth'),
  `col_31` smallint NOT NULL DEFAULT 255,
  `col_32` text ,
  `col_33` time NOT NULL DEFAULT '08:00:00',
  `col_34` timestamp NOT NULL DEFAULT '2018-01-01 00:00:01',
  `col_35` tinyblob,
  `col_36` tinyint NOT NULL DEFAULT 64,
  `col_37` tinytext ,
  `col_38` varbinary(255) NOT NULL DEFAULT 'varbinary(255)',
  `col_39` varchar(255) NOT NULL DEFAULT 'varchar(255)',
  `col_40` year NOT NULL DEFAULT 2022,
   `col_41` bit(2) NOT NULL DEFAULT b'10',
  PRIMARY KEY (`id`)
);