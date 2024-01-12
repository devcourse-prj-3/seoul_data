%%sql
WITH LatestObservations AS (
    SELECT
        category,
        base_date,
        base_time,
        obsr_value,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY created_date DESC, base_date DESC, base_time DESC) AS rn
    FROM
        hjm507.ultra_srt_ncst
    WHERE
        category IN ('T1H', 'RN1', 'REH')
),
AggregatedObservations AS (
    SELECT
        MAX(CASE WHEN category = 'T1H' THEN obsr_value END) AS t1h,
        MAX(CASE WHEN category = 'RN1' THEN obsr_value END) AS rn1,
        MAX(CASE WHEN category = 'REH' THEN obsr_value END) AS reh,
        MAX(base_date) AS latest_date,
        MAX(base_time) AS latest_time
    FROM
        LatestObservations
    WHERE
        rn = 1
)
SELECT
    ao.latest_date,
    ao.latest_time,
    ece.*,
    ao.t1h,
    ao.rn1,
    ao.reh,
    cav.MSRADMCODE,
    cav.MAXINDEX,
    cav.GRADE,
    cav.POLLUTANT,
    cav.NITROGEN,
    cav.OZONE,
    cav.CARBON,
    cav.SULFUROUS,
    cav.PM10,
    cav.PM25
FROM
    edit_cultural_event ece
JOIN
    jung_hoon_loo.city_air_v2 cav ON ece.guname = cav.msrstename
LEFT JOIN
    AggregatedObservations ao ON cav.msr_date = ao.latest_date AND cav.msr_time = ao.latest_time
WHERE
    ece.guname = '{searching}';


