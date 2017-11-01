
select year, month, dayofmonth, dayofweek, arrdelay, depdelay from ontime where year=1987;



-- 	/*
-- 	 * When is the best time of day/day of week/time of year to fly to minimise delays?'
-- 	 * 지연을 최소화하는 최적의 시간은 언제인가?
-- 	 * 매퍼 = 지연 시간, 항공기의 출발 정보(년도, 월, 일, 요일, 출발 시간 HHmm)
-- 	 */

  SELECT count(o.depdelay) Ddelay, c.description
    FROM ontime o INNER JOIN carrier c ON o.uniquecarrier = c.code
   WHERE o.year = 1987
GROUP BY c.description;

  SELECT count(o.arrdelay) Adelay, c.description
    FROM ontime o INNER JOIN carrier c ON o.uniquecarrier = c.code
   WHERE o.year = 1987
GROUP BY c.description;

  SELECT count(o.depdelay) Ddelay, count(o.arrdelay) Adelay, o.depdelay+o.arrdelay as Tdelay, c.description
    FROM ontime o INNER JOIN carrier c ON o.uniquecarrier = c.code
   WHERE o.year = 1987
GROUP BY c.description;
