-- Given a user-defined schema over a raw JSON changelog, returns the
-- schema elements of the latest set of live documents in the collection.
--   timestamp: The Firestore timestamp at which the event took place.
--   operation: One of INSERT, UPDATE, DELETE, IMPORT.
--   event_id: The event that wrote this row.
--   <schema-fields>: This can be one, many, or no typed-columns
--                    corresponding to fields defined in the schema.
SELECT document_name,
  document_id,
  timestamp,
  operation,
  INDEX_KEY,
  DATE,
  ORIG_DATE,
  FROM_CURRENCY,
  TO_CURRENCY,
  RATE,
  INFO,
  LOADED_AT,
  LOADED_FROM,
  METADATA
FROM (
    SELECT document_name,
      document_id,
      FIRST_VALUE(timestamp) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS timestamp,
      FIRST_VALUE(operation) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS operation,
      FIRST_VALUE(operation) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) = "DELETE" AS is_deleted,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.indexKey')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS INDEX_KEY,
      `invoicemaker-f5e1d.firestore_export.firestoreTimestamp`(
        FIRST_VALUE(JSON_EXTRACT(data, '$.date')) OVER(
          PARTITION BY document_name
          ORDER BY timestamp DESC
        )
      ) AS DATE,
      `invoicemaker-f5e1d.firestore_export.firestoreTimestamp`(
        FIRST_VALUE(JSON_EXTRACT(data, '$.origDate')) OVER(
          PARTITION BY document_name
          ORDER BY timestamp DESC
        )
      ) AS ORIG_DATE,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.from')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS FROM_CURRENCY,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.to')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS TO_CURRENCY,
      `invoicemaker-f5e1d.firestore_export.firestoreNumber`(
        FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.rate')) OVER(
          PARTITION BY document_name
          ORDER BY timestamp DESC
        )
      ) AS RATE,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.info')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS INFO,
      `invoicemaker-f5e1d.firestore_export.firestoreTimestamp`(
        FIRST_VALUE(JSON_EXTRACT(data, '$.loadedAt')) OVER(
          PARTITION BY document_name
          ORDER BY timestamp DESC
        )
      ) AS LOADED_AT,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.loadedFrom')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS LOADED_FROM,
      FIRST_VALUE(JSON_EXTRACT(data, '$.metadata')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS METADATA
    FROM `invoicemaker-f5e1d.firestore_export.exchangeRates_raw_latest`
  )
WHERE NOT is_deleted
GROUP BY document_name,
  document_id,
  timestamp,
  operation,
  INDEX_KEY,
  DATE,
  ORIG_DATE,
  FROM_CURRENCY,
  TO_CURRENCY,
  RATE,
  INFO,
  LOADED_AT,
  LOADED_FROM,
  METADATA