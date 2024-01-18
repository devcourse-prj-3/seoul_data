SELECT  base_date, base_time, region, t1h, rn1, reh
FROM(   SELECT  base_date, base_time, region, category, obsr_value
        FROM    hjm507.ultra_srt_ncst as u
            JOIN hjm507.region_nx_ny as r
            ON u.nx = r.nx and u.ny = r.ny)
    PIVOT (MAX(obsr_value) FOR category IN ('T1H', 'RN1', 'REH'));