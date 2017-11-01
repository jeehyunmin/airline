-------------------------------- carrier-- year

	-- delaystatics

insert overwrite local directory '/project/02_Software/portfolio/carrier/delaystatics/byyear' row format delimited fields terminated by ',' select year year, uniquecarrier code, count(depdelay) DdelayCount, avg(depdelay) DdelayAvg, count(arrdelay) AdelayCount, avg(arrdelay) AdelayAvg from ontime group by year, uniquecarrier;

	-- delayreason

insert overwrite local directory '/project/02_Software/portfolio/carrier/delayreason/byyear' row format delimited fields terminated by ',' select year, uniquecarrier code, count(carrierdelay) carriercount, avg(carrierdelay) carrieravg, sum(carrierdelay) carriersum, count(weatherdelay) weathercount, avg(weatherdelay) weatheravg, sum(weatherdelay) weathersum, count(securitydelay) securitycount, avg(securitydelay) securityavg, sum(securitydelay) securitysum, count(lateaircraftdelay) lateaircraftcount, avg(lateaircraftdelay) lateaircraftavg, sum(lateaircraftdelay) lateaircraftsum from ontime group by year, uniquecarrier;

-------------------------------- carrier-- dayofweek

	-- delaystatics
insert overwrite local directory '/project/02_Software/portfolio/carrier/delaystatics/bydayofweek' row format delimited fields terminated by ',' select dayofweek dayofWeek, uniquecarrier code, count(depdelay) DdelayCount, avg(depdelay) DdelayAvg, count(arrdelay) AdelayCount, avg(arrdelay) AdelayAvg from ontime group by dayofweek, uniquecarrier;

	-- delayreason

insert overwrite local directory '/project/02_Software/portfolio/carrier/delayreason/bydayofweek' row format delimited fields terminated by ',' select dayofweek, uniquecarrier code, count(carrierdelay) carriercount, avg(carrierdelay) carrieravg, sum(carrierdelay) carriersum, count(weatherdelay) weathercount, avg(weatherdelay) weatheravg, sum(weatherdelay) weathersum, count(securitydelay) securitycount, avg(securitydelay) securityavg, sum(securitydelay) securitysum, count(lateaircraftdelay) lateaircraftcount, avg(lateaircraftdelay) lateaircraftavg, sum(lateaircraftdelay) lateaircraftsum from ontime group by dayofweek, uniquecarrier;


-------------------------------- origin-- year

	-- delaystatics
insert overwrite local directory '/project/02_Software/portfolio/origin/delaystatics/byyear' row format delimited fields terminated by ',' select year year, origin origin, count(depdelay) DdelayCount, avg(depdelay) DdelayAvg, count(arrdelay) AdelayCount, avg(arrdelay) AdelayAvg from ontime group by year, origin;


	-- delayreason

insert overwrite local directory '/project/02_Software/portfolio/origin/delayreason/byyear' row format delimited fields terminated by ',' select year, origin origin, count(carrierdelay) carriercount, avg(carrierdelay) carrieravg, sum(carrierdelay) carriersum, count(weatherdelay) weathercount, avg(weatherdelay) weatheravg, sum(weatherdelay) weathersum, count(securitydelay) securitycount, avg(securitydelay) securityavg, sum(securitydelay) securitysum, count(lateaircraftdelay) lateaircraftcount, avg(lateaircraftdelay) lateaircraftavg, sum(lateaircraftdelay) lateaircraftsum from ontime group by year, origin;


-------------------------------- origin-- dayofweek

	-- delaystatics
insert overwrite local directory '/project/02_Software/portfolio/origin/delaystatics/bydayofweek' row format delimited fields terminated by ',' select dayofweek dayofWeek, origin origin, count(depdelay) DdelayCount, avg(depdelay) DdelayAvg, count(arrdelay) AdelayCount, avg(arrdelay) AdelayAvg from ontime group by dayofweek, origin;


	-- delayreason

insert overwrite local directory '/project/02_Software/portfolio/origin/delayreason/bydayofweek' row format delimited fields terminated by ',' select dayofweek dayofWeek, origin origin, count(carrierdelay) carriercount, avg(carrierdelay) carrieravg, sum(carrierdelay) carriersum, count(weatherdelay) weathercount, avg(weatherdelay) weatheravg, sum(weatherdelay) weathersum, count(securitydelay) securitycount, avg(securitydelay) securityavg, sum(securitydelay) securitysum, count(lateaircraftdelay) lateaircraftcount, avg(lateaircraftdelay) lateaircraftavg, sum(lateaircraftdelay) lateaircraftsum from ontime group by dayofweek, origin;



