SELECT ece.*, 
  cav.msr_date,
  cav.msr_time,
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

FROM edit_cultural_event ece
JOIN jung_hoon_loo.city_air_v2 cav ON ece.guname = cav.msrstename
WHERE ece.guname = '{searching}';
