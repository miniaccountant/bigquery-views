CREATE OR REPLACE FUNCTION `invoicemaker-f5e1d.firestore_export.firestoreGeopoint`(json STRING) RETURNS GEOGRAPHY AS (
    ST_GEOGPOINT(
      SAFE_CAST(JSON_EXTRACT(json, '$._longitude') AS NUMERIC),
      SAFE_CAST(JSON_EXTRACT(json, '$._latitude') AS NUMERIC)
    )
  );