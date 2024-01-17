SELECT ranked.*
FROM (SELECT
        e.title,
        e.guname,
        e.place,
        e.area_congest,
        e.area_congest_msg,
        e.is_free,
        p.fcst_congest_level,
        p.maxindex,
        p.grade,
        p.ozone,
        p.pm10,
        p.pm25,
        ROW_NUMBER() OVER (PARTITION BY e.title ORDER BY p.predict_time DESC) AS seq
      FROM event_population e
      JOIN predict_air p
        ON e.area_name = p.area_name AND e.guname = p.msrstename) AS ranked
WHERE seq = 1;
