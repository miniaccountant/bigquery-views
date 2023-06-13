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
  EMAIL,
  INVOICEMTX,
  FCMTOKEN,
  CUSTOM_EXCHANGE_SERVER
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
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.email')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS EMAIL,
      `invoicemaker-f5e1d.firestore_export.firestoreNumber`(
        FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.invoiceMtx')) OVER(
          PARTITION BY document_name
          ORDER BY timestamp DESC
        )
      ) AS INVOICEMTX,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.fcmToken')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS FCMTOKEN,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.theme')) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS THEME,
      FIRST_VALUE(
        JSON_EXTRACT_SCALAR(data, '$.customExchangeServer')
      ) OVER(
        PARTITION BY document_name
        ORDER BY timestamp DESC
      ) AS CUSTOM_EXCHANGE_SERVER
    FROM `invoicemaker-f5e1d.firestore_export.users_raw_latest`
  )
WHERE NOT is_deleted
GROUP BY document_name,
  document_id,
  timestamp,
  operation,
  EMAIL,
  INVOICEMTX,
  FCMTOKEN,
  CUSTOM_EXCHANGE_SERVER