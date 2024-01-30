CREATE TEMP TABLE predict_air AS (
  SELECT
    p.area_name,
    a.msrstename,
    p.fcst_yn,
    left(p.fcst_time, 10) as predict_date,
    right(p.fcst_time, 8) as predict_time,
    p.fcst_congest_level,
    a.maxindex,
    a.grade,
    a.ozone,
    a.pm10,
    a.pm25
  FROM diddmstj15.predict_redshift_in p
  JOIN jung_hoon_loo.city_air_v2 a
    ON left(p.fcst_time, 10) = a.msr_date and right(p.fcst_time, 8) = a.msr_time
);
