CREATE DATABASE IF NOT EXISTS accidents_db;
USE accidents_db;

DROP TABLE IF EXISTS accidents;

CREATE TABLE accidents (
    ID              VARCHAR(20)  PRIMARY KEY,
    Severity        INT,
    Start_Time      DATETIME,
    End_Time        DATETIME,
    Start_Lat       DOUBLE,
    Start_Lng       DOUBLE,
    Street          VARCHAR(200),
    City            VARCHAR(100),
    State           VARCHAR(10),
    Weather_Condition VARCHAR(100),
    Temperature_F   DOUBLE,
    Visibility_mi   DOUBLE,
    Junction        TINYINT(1),
    Traffic_Signal  TINYINT(1),
    Sunrise_Sunset  VARCHAR(10)
);
