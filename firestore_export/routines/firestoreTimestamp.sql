CREATE OR REPLACE FUNCTION `invoicemaker-f5e1d.firestore_export.firestoreTimestamp`(json STRING) RETURNS TIMESTAMP AS (
    TIMESTAMP_MILLIS(
      SAFE_CAST(JSON_EXTRACT(json, '$._seconds') AS INT64) * 1000 + SAFE_CAST(
        SAFE_CAST(JSON_EXTRACT(json, '$._nanoseconds') AS INT64) / 1E6 AS INT64
      )
    )
  );