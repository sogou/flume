CREATE TABLE `hive_sink_detail` (
	`id` INT(11) NOT NULL AUTO_INCREMENT,
	`name` CHAR(50) NOT NULL DEFAULT '0',
	`logdate` CHAR(12) NOT NULL DEFAULT '0',
	`hostname` CHAR(50) NOT NULL DEFAULT '0',
	`receivecount` BIGINT(20) NOT NULL DEFAULT '0',
	`sinkcount` BIGINT(20) NOT NULL DEFAULT '0',
	`updatetime` BIGINT(20) NOT NULL DEFAULT '0',
	`state` ENUM('NEW','CHECKED') NOT NULL DEFAULT 'NEW',
	PRIMARY KEY (`id`),
	INDEX `tablename` (`name`),
	INDEX `state` (`state`)
)
COLLATE='utf8_general_ci'
ENGINE=InnoDB
AUTO_INCREMENT=4;