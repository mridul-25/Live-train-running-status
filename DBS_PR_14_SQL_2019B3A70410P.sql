-- Schema
DROP DATABASE IF EXISTS train_status;
CREATE DATABASE IF NOT EXISTS train_status;
USE train_status;

CREATE TABLE IF NOT EXISTS station (
  station_code VARCHAR(4) CHECK(
    Length(station_code) >= 2
  ) PRIMARY KEY,
  station_name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS train (
  train_no INT CHECK(train_no >= 10000 AND train_no < 100000) PRIMARY KEY,
  train_name VARCHAR(30) NOT NULL,
  origin_station VARCHAR(4) NOT NULL,
  ending_station VARCHAR(4) NOT NULL,
  FOREIGN KEY(origin_station) REFERENCES station(station_code) ON DELETE CASCADE,
  FOREIGN KEY(ending_station) REFERENCES station(station_code) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS train_schedule (
    train_no INT,
    station_code VARCHAR(4),
    day_of_journey INT CHECK (day_of_journey >= 1 AND day_of_journey <= 4),
    departure_time TIME,
    arrival_time TIME,
    PRIMARY KEY (train_no, station_code),
    FOREIGN KEY(train_no) REFERENCES train (train_no) ON DELETE CASCADE,
    FOREIGN KEY(station_code) REFERENCES station (station_code) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS admin (
  train_no INT,
  station_code VARCHAR(4),
  crossing_time DATETIME,
  PRIMARY KEY (train_no, crossing_time),
  FOREIGN KEY(train_no) REFERENCES train (train_no) ON DELETE CASCADE,
  FOREIGN KEY(station_code) REFERENCES station (station_code) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS train_days (
    train_no INT,
    days INT CHECK (days IN (1, 2, 3, 4, 5, 6, 7)),
    FOREIGN KEY(train_no) REFERENCES train (train_no) ON DELETE CASCADE
);

DELIMITER $$
CREATE DEFINER='root'@'localhost'
PROCEDURE
  extractDays(IN trainNo INT)
  READS SQL DATA
  DETERMINISTIC
  SQL SECURITY INVOKER
BEGIN
  DROP TEMPORARY TABLE IF EXISTS temp_train_days;
  CREATE TEMPORARY TABLE temp_train_days (days INT);
  INSERT INTO temp_train_days
  SELECT days
  FROM   train_status.train_days
  WHERE  train_no = trainNo;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='root'@'localhost'
FUNCTION
  getArrivalTime(trainNo INT, stationCode VARCHAR(4)) RETURNS TIME
  DETERMINISTIC
BEGIN
DECLARE arrivalTime TIME;
  SET arrivalTime = (SELECT arrival_time
  FROM   train_status.train_schedule
  WHERE  train_schedule.train_no=trainNo
  AND    train_schedule.station_code = stationCode);
  RETURN arrivalTime;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='root'@'localhost'
FUNCTION
  getDepartureTime(trainNo INT, stationCode VARCHAR(4)) RETURNS TIME
  DETERMINISTIC
BEGIN
DECLARE departureTime TIME;
  SET departureTime = (SELECT departure_time
  FROM   train_status.train_schedule
  WHERE  train_no=trainNo
  AND    station_code = stationCode);
  RETURN departureTime;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='root'@'localhost'
FUNCTION
  getCrossingTime(trainNo INT, stationCode VARCHAR(4)) RETURNS DATETIME
  DETERMINISTIC
BEGIN
  DECLARE crossingTime DATETIME;
  SET crossingTime = (SELECT crossing_time
  FROM   train_status.admin
  WHERE  train_no=trainNo
  AND    station_code= stationCode
  ORDER BY crossing_time DESC
  LIMIT 1);
  RETURN crossingTime;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='root'@'localhost'
FUNCTION
  getJourneyStartTime(trainNo INT) RETURNS DATETIME
  DETERMINISTIC
BEGIN
  DECLARE startTime DATETIME;
  DECLARE depTime TIME;
  SET depTime = (SELECT departure_time
  FROM   train_status.train_schedule
  WHERE  train_no=trainNo AND station_code = (SELECT origin_station FROM train_status.train WHERE train.train_no = trainNo));
  IF depTime < CURTIME()
  THEN SET startTime = CURRENT_TIMESTAMP() - CURTIME() + depTime;
  ELSE SET startTime = CURRENT_TIMESTAMP() - INTERVAL 1 DAY - CURTIME() + depTime;
  END IF;
  RETURN startTime;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='root'@'localhost'
FUNCTION
  getOriginStation(trainNo INT) RETURNS VARCHAR(4)
  DETERMINISTIC
BEGIN
  DECLARE firstStation VARCHAR(4);
  SET firstStation = (SELECT origin_station
  FROM   train_status.train
  WHERE  train_no=trainNo);
  RETURN firstStation;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='root'@'localhost'
FUNCTION
  getDestinationStation(trainNo INT) RETURNS VARCHAR(4)
  DETERMINISTIC
BEGIN
  DECLARE lastStation VARCHAR(4);
  SET lastStation = (SELECT ending_station
  FROM   train_status.train
  WHERE  train_no=trainNo);
  RETURN lastStation;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='root'@'localhost'
FUNCTION
  getLastStationCrossed(trainNo INT) RETURNS VARCHAR(4)
  DETERMINISTIC
BEGIN
  DECLARE stationCrossed VARCHAR(4) DEFAULT NULL;
  SET stationCrossed = (SELECT station_code
  FROM   train_status.admin
  WHERE  train_no=trainNo
  ORDER BY crossing_time DESC
  LIMIT 1);
  RETURN stationCrossed;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='root'@'localhost' FUNCTION
getTimeDelay(departureTime TIME, crossingTime TIME) RETURNS VARCHAR(50)
 DETERMINISTIC
BEGIN
 DECLARE delayTime TIME;
 DECLARE returnMessage VARCHAR(50) DEFAULT NULL;
 IF crossingTime >= departureTime
 THEN SET delayTime = SUBTIME(crossingTime, departureTime);
 ELSE SET delayTime = SUBTIME(ADDTIME(crossingTime, "24:00:00"), departureTime);
 END IF;
 IF TIME_TO_SEC(delayTime) >= 60 THEN
 SET returnMessage = CONCAT("The train is behind by ", HOUR(delayTime), " hours and ", MINUTE(delayTime), " minutes.");
 ELSEIF TIME_TO_SEC(delayTime) <= -60 THEN
 SET returnMessage = CONCAT("The train is ahead by ", HOUR(delayTime), " hours and ", MINUTE(delayTime), " minutes.");
 ELSE SET returnMessage = "The train is on time.";
 END IF;
 RETURN returnMessage;
 END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='root'@'localhost' FUNCTION
checkTrainExists(trainNo INT) RETURNS INT
 DETERMINISTIC
BEGIN
 DECLARE exist INT DEFAULT 0;
 IF (SELECT COUNT(*) FROM train_status.train WHERE train_no = trainNo) != 0
 THEN SET exist = 1;
 
 END IF;
 return exist;
 END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='root'@'localhost'
FUNCTION
  getTrainStatus(trainNo INT) RETURNS VARCHAR(50)
  DETERMINISTIC
BEGIN
  DECLARE returnMessage VARCHAR(50) DEFAULT "";
  DECLARE stationCrossed VARCHAR(4) DEFAULT NULL;
  DECLARE startingTime DATETIME DEFAULT NULL;
  DECLARE crossingTime DATETIME;
  DECLARE scheduledDepartureTime TIME;
  IF checkTrainExists(trainNo) != 0 THEN
    SET stationCrossed = getLastStationCrossed(trainNo);
    SET startingTime = getJourneyStartTime(trainNo);
    SET crossingTime = getCrossingTime(trainNo, stationCrossed);
    SET scheduledDepartureTime = getDepartureTime(trainNo, stationCrossed);
    IF stationCrossed IS NULL OR stationCrossed = getDestinationStation(trainNo)
    THEN SET returnMessage = "Train Journey is complete or not yet started.";
    -- ELSEIF scheduledDepartureTime < TIME(startingTime) 
    ELSE SET returnMessage = getTimeDelay(scheduledDepartureTime, TIME(crossingTime));
    -- ELSE SET returnMessage = getTimeDelay(CURRENT_TIMESTAMP() - CURTIME() + scheduledDepartureTime ,crossingTime);
    END IF;
  END IF;
  RETURN returnMessage;
END$$
DELIMITER ;


DELIMITER $$
CREATE DEFINER='root'@'localhost'
PROCEDURE
  adminUpdate(IN trainNo INT,in stationCode varchar(4),in crossingTime datetime)
  modifies SQL DATA
  DETERMINISTIC
  SQL SECURITY INVOKER
BEGIN
  insert into train_status.admin values (trainNo, stationCode, crossingTime);
END$$
DELIMITER ;

-- 1 -> Train scheduled for given date
-- 0 -> Train not scheduled for given date
DELIMITER $$
CREATE DEFINER='root'@'localhost'
FUNCTION
  checkDayOfJourney(inputDate date, trainNo int, startStation VARCHAR(4)) RETURNS INT
  DETERMINISTIC
BEGIN
  declare dayNumber int default dayofweek(inputDate);
  DECLARE journeyDayOfStation INT DEFAULT 1;
  DECLARE trainOnDay INT DEFAULT 0;
  SET journeyDayOfStation = (SELECT day_of_journey FROM train_schedule WHERE train_no = trainNo AND station_code = startStation);
  call extractDays(trainNo);
  IF dayNumber IN (SELECT days + journeyDayOfStation - 1 FROM temp_train_days)
  THEN SET trainOnDay = 1;
  ELSE SET trainOnDay = 0;
  END IF;
  RETURN trainOnDay;
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='root'@'localhost'
PROCEDURE
  trainsBetweenStations(IN startStation VARCHAR(4),IN endStation VARCHAR(4))
  READS SQL DATA
  DETERMINISTIC
  SQL SECURITY INVOKER
BEGIN
  SELECT ts1.train_no train_no, ts1.departure_time, ts2.arrival_time FROM train_schedule ts1, train_schedule ts2
  WHERE (ts1.train_no = ts2.train_no)
	AND (ts1.station_code = startStation) AND (ts2.station_code = endStation)
    AND (ts1.day_of_journey < ts2.day_of_journey OR (ts1.day_of_journey = ts2.day_of_journey AND ts1.departure_time < ts2.arrival_time));
END$$
DELIMITER ;

DELIMITER $$
CREATE DEFINER='root'@'localhost'
PROCEDURE
  trainsBetweenStationsOnDate(startStation VARCHAR(4), endStation VARCHAR(4), inputDate date)
  READS SQL DATA
  DETERMINISTIC
  SQL SECURITY INVOKER
BEGIN
  SELECT ts1.train_no train_no, ts1.departure_time, ts2.arrival_time FROM train_schedule ts1, train_schedule ts2
  WHERE (ts1.train_no = ts2.train_no)
	AND (ts1.station_code = startStation) AND (ts2.station_code = endStation)
    AND (ts1.day_of_journey < ts2.day_of_journey OR (ts1.day_of_journey = ts2.day_of_journey AND ts1.departure_time < ts2.arrival_time))
    AND (checkDayOfJourney(inputDate, ts1.train_no, startStation) = 1);
END$$
DELIMITER ;

-- Inserting Values
USE train_status;

INSERT INTO station VALUES
('NDLS','New Delhi Station'),
('CNB','Kanpur Central'),
('PNKD','Panki Dham'),
('BDTS', 'Bandra Terminus'),
('BVI', 'Borivali'),
('ST', 'Surat'),
('GDA', 'Godhra Junction'),
('RTM', 'Ratlam Junction'),
('MTJ', 'Mathura Junction'),
('FDB', 'Faridabad'),
('NZM', 'Hazrat Nizamuddin'),
('SNP', 'Sonipat Junction'),
('PNP', 'Paniput Junction'),
('KUN', 'Karnal'),
('KKDE', 'Kurukshetra Junction'),
('UMB', 'Ambala Cantt Junction'),
('CDG','Chandigarh'),
('LDH','Ludhiana Junction'),
('JUC', 'Jalandhar City'),
('ASR', 'Amritsar Junction'),
('PRYJ','Prayagraj Junction'),
('CSMT', 'Chhatrapati Shivaji Maharaj Junction'),
('BSB','Varanasi Junction'),
('PUNE','Pune junction'),
('DR','Dadar'),
('TNA','Thane'),
('LNL','Lonavala'),
('SVJR','Shivaji Nagar'),
('ADI','Ahmedabad Junction'),
('BRC','Vadodara Junction'),
('HWH','Howrah Junction'),
('UDN','Udhna Jn (Surat)'),
('NGP','Nagpur RL'),
('DURG','Durg RL'),
('BSP','Bilaspur JN'),
('CPH','Champa'),
('ROU','Rourkela'),
('KGP','Kharagpur Jn'),
('MAS','Mgr Chennai Ctr'),
('WL','Warangal'),
('BPL','Bhopal Junction'),
('UJN','Ujjain Junction'),
('KOTA','Kota Junction'),
('DPA','Durgapura'),
('JP','Jaipur');

INSERT INTO train VALUES
(12451, "Shram Shakti Express", "CNB", "NDLS"),
(12452, "Shram Shakti Express", "NDLS", "CNB"),
(22436, "Vande Bharat Express", "NDLS", "PRYJ"),
(12925, "Paschim SF Express", "BDTS", "ASR"),
(12127, "Pune Intercity SF Express", "CSMT", "PUNE"),
(12833, "Howrah SF Express", "ADI", "HWH"),
(12967, "Jaipur SF Express", "MAS", "JP");

INSERT INTO train_schedule VALUES
(12451, 'CNB', 1, '23:55:00', null),
(12451, 'PNKD', 2, '00:14:00', '00:13:00'),
(12451, 'NDLS', 2, null, '05:50:00');

INSERT INTO train_schedule VALUES
(12452, 'NDLS', 1, '23:55:00', null),
(12452, 'PNKD', 2, '05:10:00', '05:09:00'),
(12452, 'CNB', 2, null, '06:00:00');

INSERT INTO train_schedule VALUES
(22436, 'NDLS', 1, '06:00:00', null),
(22436, 'CNB', 1, '10:10:00', '10:08:00'),
(22436, 'PRYJ', 1, '12:10:00', '12:08:00'),
(22436, 'BSB', 1, null, '14:00:00');

INSERT INTO train_schedule VALUES
(12925, 'BDTS', 1, '11:30:00', null),
(12925, 'BVI', 1, '11:58:00', '11:55:00'),
(12925, 'ST', 1, '15:52:00', '15:47:00'),
(12925, 'GDA', 1, '19:00:00', '18:58:00'),
(12925, 'RTM', 1, '21:45:00', '21:40:00'),
(12925, 'KOTA', 2, '01:55:00', '01:45:00'),
(12925, 'MTJ', 2, '07:40:00', '07:35:00'),
(12925, 'FDB', 2, '09:29:00', '09:27:00'),
(12925, 'NZM', 2, '10:01:00', '09:59:00'),
(12925, 'NDLS', 2, '11:05:00', '10:40:00'),
(12925, 'SNP', 2, '11:52:00', '11:50:00'),
(12925, 'PNP', 2, '12:25:00', '12:23:00'),
(12925, 'KUN', 2, '12:51:00', '12:49:00'),
(12925, 'KKDE', 2, '13:16:00', '13:14:00'),
(12925, 'UMB', 2, '14:40:00', '14:30:00'),
(12925, 'CDG', 2, '15:25:00', '15:20:00'),
(12925, 'LDH', 2, '17:45:00', '17:35:00'),
(12925, 'JUC', 2, '18:50:00', '18:45:00'),
(12925, 'ASR', 2, null, '20:15:00');

INSERT INTO train_schedule (train_no, station_code, day_of_journey, arrival_time, departure_time) VALUES
(12967,'MAS',1,'17:40:00',null),
(12967,'WL',2,'2:54:00','2:56:00'),
(12967,'NGP',2,'11:10:00','11:15:00'),
(12967,'BPL',2,'17:50:00','18:00:00'),
(12967,'UJN',2,'21:15:00','21:25:00'),
(12967,'KOTA',3,'02:25:00','02:35:00'),
(12967,'DPA',3,'06:01:00','06:04:00'),
(12967,'JP',3,null,'6:45:00');

INSERT INTO train_schedule (train_no, station_code, day_of_journey, arrival_time, departure_time) VALUES
(12833,'ADI',1,'00:25:00',null),
(12833,'BRC',1,'2:04:00','2:11:00'),
(12833,'HWH',2,null,'13:35:00'),
(12833,'UDN',1,'4:42:00','4:44:00'),
(12833,'NGP',1,'18:00:00','18:05:00'),
(12833,'DURG',1,'22:30:00','22:35:00'),
(12833,'BSP',2,'1:30:00','1:35:00'),
(12833,'CPH',2,'2:36:00','2:39:00'),
(12833,'ROU',2,'6:45:00','6:53:00'),
(12833,'KGP',2,'11:25:00','11:30:00');

INSERT INTO train_schedule (train_no, station_code, day_of_journey, arrival_time, departure_time) VALUES
(12127,'CSMT', 1,'6:40:00',null),
(12127,'PUNE',1,null,'9:57'),
(12127,'DR',1,'6:46:00','6:48:00'),
(12127,'TNA',1,'7:08:00','7:10:00'),
(12127,'LNL',1,'8:48:00','8:50:00'),
(12127,'SVJR',1,'9:39:00','9:40:00');

INSERT INTO train_days VALUES
(12451, 1),
(12451, 2),
(12451, 3),
(12451, 4),
(12451, 5),
(12451, 6),
(12451, 7);

INSERT INTO train_days VALUES
(12452, 1),
(12452, 2),
(12452, 3),
(12452, 4),
(12452, 5),
(12452, 6),
(12452, 7);

INSERT INTO train_days VALUES
(22436, 1),
(22436, 3),
(22436, 4),
(22436, 6),
(22436, 7);

INSERT INTO train_days VALUES
(12925, 1),
(12925, 2),
(12925, 3),
(12925, 4),
(12925, 5),
(12925, 6),
(12925, 7);

INSERT INTO train_days VALUES
(12967, 1),
(12967, 3);

INSERT INTO train_days VALUES
(12833, 1),
(12833, 2),
(12833, 3),
(12833, 4),
(12833, 5),
(12833, 6),
(12833, 7);

INSERT INTO train_days VALUES
(12127, 1),
(12127, 2),
(12127, 3),
(12127, 4),
(12127, 5),
(12127, 6),
(12127, 7);


-- Test Queries
CALL trainsBetweenStationsOnDate("NGP", "KOTA", "2022-04-13");
CALL trainsBetweenStationsOnDate("CNB", "PRYJ", "2022-04-11");
SELECT checkDayOfJourney(DATE("2022-04-11"), 12967, "NGP");
SELECT subtime(addtime("00:10:00", "24:00:00"), "23:55:00");
SELECT getArrivalTime(12967, 'WL');
SELECT getDepartureTime(12925, 'JUC');
CALL adminUpdate(12833, 'UDN', "04:44:00");
SELECT * FROM admin;
SELECT getCrossingTime(12925, 'GDA');
SELECT getTimeDelay("23:55:00", "23:10:00");
SELECT getTrainStatus(12833);

start transaction;
CALL adminUpdate(12833, 'UDN', "04:44:00");
commit;
start transaction;
CALL trainsBetweenStationsOnDate("NGP", "KOTA", "2022-04-13");
CALL trainsBetweenStationsOnDate("CNB", "PRYJ", "2022-04-10");
commit;
start transaction;
SELECT getTrainStatus(12833);
commit;

USE train_status;

select * from train;
select * from station;
select * from train_schedule;
select * from train_days;
select * from admin;
SELECT getArrivalTime(12967, 'WL'); -- showing the values of arrival time
SELECT getDepartureTime(12925, 'JUC'); -- showing the values of the departure time
SELECT getTimeDelay("23:55:00", "23:10:00"); -- describe the function first.
CALL trainsBetweenStations("NDLS", "BSB");-- output is there
CALL trainsBetweenStations("BSB", "NDLS");
CALL trainsBetweenStations("NDLS","JP"); -- no output is there
select checkTrainExists(22334); -- 0 means no train exists

start transaction;
call adminupdate(12967,'WL','2022-04-10 2:58:00');
call adminupdate(12967,'NGP','2022-04-10 11:25:00');
call adminupdate(12967,'BPL','2022-04-10 18:00:00');
call adminupdate(12967,'UJN','2022-04-10 21:30:00');
call adminupdate(12967,'KOTA','2022-04-11 02:50:00');
call adminupdate(12967,'DPA','2022-04-11 06:04:00');
commit;

start transaction;
select gettrainstatus(12967);
commit;
-- now show difference between trainsBetweenStations and trainsBetweenStattionsDate one

start transaction;
CALL trainsBetweenStationsOnDate("NGP", "KOTA", "2022-04-13");

CALL trainsBetweenStationsOnDate("CNB", "PRYJ", "2022-04-11");
call trainsBetweenStations('CNB',"PRYJ");
commit;

-- DROP DATABASE train_status;