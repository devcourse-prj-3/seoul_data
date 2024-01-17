WITH RankedData AS (
    SELECT
        rpv.conn_time,
        rpv.guname,
        rpv.area_name,
        rpv.area_congest,
        cul.codename,
        cul.title,
        cul.date,
        cul.place,
        ultra.obsr_value AS TEMP,
        ROW_NUMBER() OVER (PARTITION BY rpv.area_name ORDER BY rpv.conn_time DESC) AS RowNum
    FROM
        jang_jungbin.realtime_population_v2 AS rpv
    LEFT JOIN
        jang_jungbin.region_nx_ny AS region
    ON
        rpv.guname = region.region
    LEFT JOIN
        jang_jungbin.cultural_event AS cul
    ON
        rpv.guname = cul.guname
    LEFT JOIN
        jang_jungbin.ultra_srt_ncst AS ultra
    ON
        region.nx = ultra.nx
        AND region.ny = ultra.ny
    WHERE
       ultra.category = 'T1H'
)
SELECT
    conn_time,
    guname,
    area_name,
    area_congest,
    Temp,
    codename,
    title,
    date,
    place
FROM
    RankedData
WHERE
    RowNum = 1
    AND title != 'NONE'