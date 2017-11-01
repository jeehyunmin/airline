insert overwrite local directory '/tmp/result' 
row format delimited
fields terminated by ','
select dayofweek, SUM(arrdelay+depdelay) as sum_delay, count(*) from ontime 
where year=1987 group by dayofweek;


-- test--

	-- 항공사 지연 통계

	-- Table delaystatics created
create table delaystatics (Year int, Uniquecarrier varchar(4), DdelayCount int, DdelayAvg float, AdelayCount int, AdelayAvg float);

	-- insert data into tables
insert overwrite table delaystatics 
select year year, uniquecarrier code, 
count(depdelay) DdelayCount, avg(depdelay) DdelayAvg,
count(arrdelay) AdelayCount, avg(arrdelay) AdelayAvg 
from ontime group by year, uniquecarrier;

	--DELAYStatics & local save

insert overwrite local directory '/project/02_Software/portfolio/delaystatics' row format delimited fields terminated by ',' select year year, uniquecarrier code, count(depdelay) DdelayCount, avg(depdelay) DdelayAvg, count(arrdelay) AdelayCount, avg(arrdelay) AdelayAvg from ontime group by year, uniquecarrier;


	-- 지연 사유 별 항공사의 연도별 데이터
insert overwrite local directory '/tmp/result/delay'
row format delimited fields terminated by ','
select year, uniquecarrier, count(carrierdelay) carriercount, avg(carrierdelay) carrieravg, sum(carrierdelay) carriersum, count(weatherdelay) weathercount, avg(weatherdelay) weatheravg, sum(weatherdelay) weathersum, count(securitydelay) securitycount, avg(securitydelay) securityavg, sum(securitydelay) securitysum, count(lateaircraftdelay) lateaircraftcount, avg(lateaircraftdelay) lateaircraftavg, sum(lateaircraftdelay) lateaircraftsum from ontime group by year, uniquecarrier;

insert overwrite local directory '/tmp/result/delay'
row format delimited fields terminated by ','
select uniquecarrier, count(carrierdelay), avg(carrierdelay), sum(carrierdelay), count(weatherdelay), avg(weatherdelay), sum(weatherdelay), count(securitydelay), avg(securitydelay), sum(securitydelay), count(lateaircraftdelay), avg(lateaircraftdelay), sum(lateaircraftdelay) from ontime group by uniquecarrier;

select uniquecarrier code, count(depdelay) DdelayCount, avg(depdelay) DdelayAvg, count(arrdelay) AdelayCount, avg(arrdelay) AdelayAvg from ontime where year = 1987 group by uniquecarrier order by uniquecarrier asc;


 ---Table delayreason created

create table delayreason(year int, uniquecarrier varchar(4), carriercount int, carrieravg float, carriersum float, weathercount int, weatheravg float, weathersum float, securitycount int, securityavg float, securitysum float, lateaircraftcount int, lateaircraftavg float, lateaircraftsum float);

	--delayreason from ontime

insert overwrite table delayreason select year, uniquecarrier, count(carrierdelay) carriercount, avg(carrierdelay) carrieravg, sum(carrierdelay) carriersum, count(weatherdelay) weathercount, avg(weatherdelay) weatheravg, sum(weatherdelay) weathersum, count(securitydelay) securitycount, avg(securitydelay) securityavg, sum(securitydelay) securitysum, count(lateaircraftdelay) lateaircraftcount, avg(lateaircraftdelay) lateaircraftavg, sum(lateaircraftdelay) lateaircraftsum from ontime group by year, uniquecarrier;

	-- DELAYSTATICS & LOCAL SAVE

insert overwrite local directory '/project/02_Software/portfolio' row format delimited fields terminated by ',' select * from delaystatics;


	-- DELAYREASON & local save

insert overwrite local directory '/project/02_Software/portfolio/delayreason' row format delimited fields terminated by ',' select year, origin, count(carrierdelay) carriercount, avg(carrierdelay) carrieravg, sum(carrierdelay) carriersum, count(weatherdelay) weathercount, avg(weatherdelay) weatheravg, sum(weatherdelay) weathersum, count(securitydelay) securitycount, avg(securitydelay) securityavg, sum(securitydelay) securitysum, count(lateaircraftdelay) lateaircraftcount, avg(lateaircraftdelay) lateaircraftavg, sum(lateaircraftdelay) lateaircraftsum from ontime group by year, origin;

-------------------------------- carrier-- year

	-- origindelaystatics

insert overwrite local directory '/project/02_Software/portfolio/carrier/delaystatics/byyear' row format delimited fields terminated by ',' select year year, uniquecarrier code, count(depdelay) DdelayCount, avg(depdelay) DdelayAvg, count(arrdelay) AdelayCount, avg(arrdelay) AdelayAvg from ontime group by year, uniquecarrier;

	-- origindelayreason

insert overwrite local directory '/project/02_Software/portfolio/carrier/delayreason/byyear' row format delimited fields terminated by ',' select year, uniquecarrier code, count(carrierdelay) carriercount, avg(carrierdelay) carrieravg, sum(carrierdelay) carriersum, count(weatherdelay) weathercount, avg(weatherdelay) weatheravg, sum(weatherdelay) weathersum, count(securitydelay) securitycount, avg(securitydelay) securityavg, sum(securitydelay) securitysum, count(lateaircraftdelay) lateaircraftcount, avg(lateaircraftdelay) lateaircraftavg, sum(lateaircraftdelay) lateaircraftsum from ontime group by year, uniquecarrier;

-------------------------------- carrier-- dayofweek

	-- origindelaystatics
insert overwrite local directory '/project/02_Software/portfolio/carrier/delaystatics/bydayofweek' row format delimited fields terminated by ',' select dayofweek dayofWeek, uniquecarrier code, count(depdelay) DdelayCount, avg(depdelay) DdelayAvg, count(arrdelay) AdelayCount, avg(arrdelay) AdelayAvg from ontime group by dayofweek, uniquecarrier;

	-- origindelayreason

insert overwrite local directory '/project/02_Software/portfolio/carrier/delayreason/bydayofweek' row format delimited fields terminated by ',' select year, origin, count(carrierdelay) carriercount, avg(carrierdelay) carrieravg, sum(carrierdelay) carriersum, count(weatherdelay) weathercount, avg(weatherdelay) weatheravg, sum(weatherdelay) weathersum, count(securitydelay) securitycount, avg(securitydelay) securityavg, sum(securitydelay) securitysum, count(lateaircraftdelay) lateaircraftcount, avg(lateaircraftdelay) lateaircraftavg, sum(lateaircraftdelay) lateaircraftsum from ontime group by year, origin;


-------------------------------- origin-- year -- origin

	-- origindelaystatics
insert overwrite local directory '/project/02_Software/portfolio/carrier/delaystatics/bydayofweek' row format delimited fields terminated by ',' select dayofweek dayofWeek, uniquecarrier code, count(depdelay) DdelayCount, avg(depdelay) DdelayAvg, count(arrdelay) AdelayCount, avg(arrdelay) AdelayAvg from ontime group by dayofweek, uniquecarrier;


