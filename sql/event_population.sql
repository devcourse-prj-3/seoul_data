CREATE TEMP TABLE event_population AS (
  SELECT
    e.title,
    p.guname,
    e.place,
    e.strtdate,
    e.end_date,
    p.area_congest,
    p.area_congest_msg,
    e.is_free,
    p.area_name
  FROM jang_jungbin.realtime_population_v2 p
  JOIN jang_jungbin.cultural_event e
    ON p.guname = e.guname AND left(p.conn_time, 10) BETWEEN e.strtdate AND e.end_date
);
