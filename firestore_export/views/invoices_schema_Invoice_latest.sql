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
  NUMBER,
  DATE,
  POSITIONS,
  TRANSACTIONS,
  CUSTOMER,
  CURRENCY,
  USER
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
      `invoicemaker-f5e1d.firestore_export.firestoreNumber`(
        FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.number')) OVER(
          PARTITION BY document_name
          ORDER BY timestamp DESC
        )
      ) AS NUMBER,
      `invoicemaker-f5e1d.firestore_export.firestoreTimestamp`(
        FIRST_VALUE(JSON_EXTRACT(data, '$.date')) OVER(
          PARTITION BY document_name
          ORDER BY timestamp DESC
        )
      ) AS DATE,
      FIRST_VALUE(JSON_EXTRACT(data, '$.positions')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS POSITIONS,
      FIRST_VALUE(JSON_EXTRACT(data, '$.transactions')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS TRANSACTIONS,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.customer')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS CUSTOMER,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.currency')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS CURRENCY,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.user')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS USER
    FROM `invoicemaker-f5e1d.firestore_export.invoices_raw_latest`
  )
WHERE NOT is_deleted
GROUP BY document_name,
  document_id,
  timestamp,
  operation,
  NUMBER,
  DATE,
  POSITIONS,
  TRANSACTIONS,
  CUSTOMER,
  CURRENCY,
  USER