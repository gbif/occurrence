-- Convert to JSON using this:
-- grep -v -- '--' tech-docs-poland-cube.sql | jq -R -s .
SELECT
  -- Dimensions:
  PRINTF('%04d-%02d', "year", "month") AS yearMonth,
  GBIF_EEARGCode(
    1000,
    decimalLatitude,
    decimalLongitude,
    COALESCE(coordinateUncertaintyInMeters, 1000)
  ) AS eeaCellCode,
  familyKey,
  family,
  speciesKey,
  species,
  COALESCE(occurrence.sex.concept, 'NOT_SUPPLIED') AS sex,
  COALESCE(occurrence.lifestage.concept, 'NOT_SUPPLIED') AS lifestage,
  -- Measurements
  COUNT(*) AS occurrences,
  MIN(COALESCE(coordinateUncertaintyInMeters, 1000)) AS minCoordinateUncertaintyInMeters,
  MIN(GBIF_TemporalUncertainty(eventDate)) AS minTemporalUncertainty,
  -- Higher taxon measurement
  IF(ISNULL(familyKey), NULL, SUM(COUNT(*)) OVER (PARTITION BY familyKey)) AS familyCount
FROM
  occurrence
WHERE occurrenceStatus = 'PRESENT'
  AND countryCode = 'PL'
  AND "year" >= 2000
  AND kingdomKey = 1
  AND hasCoordinate = TRUE
  AND speciesKey IS NOT NULL
  AND NOT ARRAY_CONTAINS(issue, 'ZERO_COORDINATE')
  AND NOT ARRAY_CONTAINS(issue, 'COORDINATE_OUT_OF_RANGE')
  AND NOT ARRAY_CONTAINS(issue, 'COORDINATE_INVALID')
  AND NOT ARRAY_CONTAINS(issue, 'COUNTRY_COORDINATE_MISMATCH')
  AND "month" IS NOT NULL
GROUP BY
  yearMonth,
  eeaCellCode,
  familyKey,
  family,
  speciesKey,
  species,
  sex,
  lifestage
ORDER BY
  yearMonth DESC,
  eeaCellCode ASC,
  speciesKey ASC;
