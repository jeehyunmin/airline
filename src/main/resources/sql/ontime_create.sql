
/*
 * ontime table
*/

-- DROP TABLE ontime;
-- truncate table ontime;
-- delete from ontime;

create table ontime (
  Year int,
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime  int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier varchar(5),
  FlightNum int,
  TailNum varchar(8),
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin varchar(3),
  Dest varchar(3),
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled int,
  CancellationCode varchar(1),
  Diverted varchar(1),
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
);


/* 
 * airport table
 */

-- drop table airport;
-- truncate airport;

create table airport (
	iata 		varchar(10) PRIMARY KEY ,
	airport 	varchar(100),
	city		varchar(100),
	state		varchar(50),
	country	varchar(50),
	lat			numeric(11,8),
	lng			numeric(11,8)
);

select * from airport;

select o.`Year`, o.`Origin`, a.iata, a.airport 
  from ontime o inner join airport a
    on o.`Origin` = a.iata
 where o.`Year` = 1987;


/* 
 * carrier table
 */

drop table carrier;
-- truncate table carrier;

create table carrier (
	Code			varchar(5) primary key,
	Description	varchar(100)
	);
	
select count(*) from carrier;

select o.`Year`, o.`UniqueCarrier`, c.`Code`, c.`Description`
  from ontime o inner join carrier c
    on o.`UniqueCarrier` = c.`Code`
 where o.`Year` = 1987;

/* 
 * plane table
 */
 
-- drop table plane;
-- truncate plane;
-- delete from plane;
 
 create table plane (
		 tailnum 			varchar(8) primary key,
		 type 				varchar(100),
		 manufacturer		varchar(100),
		 issue_date		varchar(100),
		 model				varchar(100),
		 status			varchar(100),
		 aircraft_type	varchar(100),
		 engine_type		varchar(100),
		 year				varchar(4)
 );

select * from plane;