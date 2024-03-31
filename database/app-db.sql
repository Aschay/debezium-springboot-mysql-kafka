CREATE DATABASE IF NOT EXISTS `customerdb` ;
USE `customerdb`;
DROP TABLE IF EXISTS `customer`;
CREATE TABLE `customer` (
  `id` varchar(36) NOT NULL,
  `email` varchar(255) DEFAULT NULL,
  `username` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO `customer` (`id`,`email`,`username`)VALUES(UUID (),'will.smith@gmail.Com','smithy114');
INSERT INTO `customer` (`id`,`email`,`username`)VALUES(UUID (),'jannet.smith@gmail.Com','jss178');
