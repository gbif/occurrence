-- Convert to JSON using this:
-- grep -v -- '--' test-download-query-sql-all-functions.sql | jq -R -s .

SELECT
  -- Dimensions:
  PRINTF('%04d-%02d', "year", "month") AS yearMonth,

  -- Grid cells
  GBIF_EEARGCode(1000, decimalLatitude, decimalLongitude, COALESCE(coordinateUncertaintyInMeters, 1000)) AS eeaCellCode,
  GBIF_EQDGCode(    2, decimalLatitude, decimalLongitude, COALESCE(coordinateUncertaintyInMeters, 1000)) AS eqdgCellCode,
  GBIF_DMSGCode(  300, decimalLatitude, decimalLongitude, COALESCE(coordinateUncertaintyInMeters, 1000)) AS dmsgCellCode,
  GBIF_ISEA3HCode(  6, decimalLatitude, decimalLongitude, COALESCE(coordinateUncertaintyInMeters, 1000)) AS isea3hCellCode,
  GBIF_MGRSCode( 1000, decimalLatitude, decimalLongitude, COALESCE(coordinateUncertaintyInMeters, 1000)) AS mgrsCellCode,

  familyKey,
  family,
  speciesKey,
  species,

  -- Text output functions
  CONCAT_WS(' | ', occurrence.typestatus.concepts) AS typeStatuses,
  CONCAT_WS(' | ', occurrence.typestatus.lineage) AS typeStatusLineages,
  COALESCE(occurrence.sex.concept, 'NOT_SUPPLIED') AS sex,

  -- Measurements
  COUNT(*) AS occurrences,
  MIN(COALESCE(coordinateUncertaintyInMeters, 1000)) AS minCoordinateUncertaintyInMeters,
  MIN(GBIF_TemporalUncertainty(eventDate, null)) AS minTemporalUncertaintyDateOnly,
  MIN(GBIF_TemporalUncertainty(eventDate, eventTime)) AS minTemporalUncertaintyDateTime,
  GBIF_SecondsToLocalISO8601(MAX(eventDateLte)) AS latestDate,
  GBIF_MillisecondsToISO8601(MAX(modified)) AS latestModified,
  GBIF_SecondsToISO8601(MAX(dateIdentified)) AS identifiedDate
FROM
  occurrence
WHERE occurrenceStatus = 'PRESENT'
  AND "year" >= 1900
  AND hasCoordinate = TRUE
  AND (coordinateUncertaintyInMeters <= 1000 OR coordinateUncertaintyInMeters IS NULL)
  AND speciesKey IS NOT NULL
  AND NOT GBIF_STRINGARRAYCONTAINS(occurrence.issue, 'TAXON_MATCH_FUZZY', TRUE)
  AND NOT ARRAY_CONTAINS(issue, 'ZERO_COORDINATE')
  AND NOT GBIF_StringArrayContains(occurrence.recordedby, 'Matthew', FALSE)
  AND "month" IS NOT NULL
  AND (GBIF_GeoDistance(56.0, 12.0, '10km', decimalLatitude, decimalLongitude) = TRUE
    OR GBIF_Within('POLYGON ((10.6 57.9, 7.6 57.0, 7.4 55.0, 10.5 54.5, 11.1 56.6, 10.6 57.9))', decimalLatitude, decimalLongitude) = TRUE)
GROUP BY
  yearMonth,
  eeaCellCode,
  eqdgCellCode,
  dmsgCellCode,
  isea3hCellCode,
  mgrsCellCode,
  familyKey,
  family,
  speciesKey,
  species,
  typeStatuses,
  typeStatusLineages,
  sex
ORDER BY
  yearMonth DESC,
  eeaCellCode ASC,
  speciesKey ASC;
