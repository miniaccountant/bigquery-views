-- Retrieves the latest document change events for all live documents.
--   timestamp: The Firestore timestamp at which the event took place.
--   operation: One of INSERT, UPDATE, DELETE, IMPORT.
--   event_id: The id of the event that triggered the cloud function mirrored the event.
--   data: A raw JSON payload of the current state of the document.
--   document_id: The document id as defined in the Firestore database
SELECT
  document_name,
  document_id,
  timestamp,
  event_id,
  operation,
  data,
  old_data
FROM
  (
    SELECT
      document_name,
      document_id,
      FIRST_VALUE(timestamp) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS timestamp,
      FIRST_VALUE(event_id) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS event_id,
      FIRST_VALUE(operation) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS operation,
      FIRST_VALUE(data) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS data,
      FIRST_VALUE(old_data) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS old_data,
      FIRST_VALUE(operation) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) = "DELETE" AS is_deleted
    FROM
      `invoicemaker-f5e1d.firestore_export.services_raw_changelog`
    ORDER BY
      document_name,
      timestamp DESC
  )
WHERE
  NOT is_deleted
GROUP BY
  document_name,
  document_id,
  timestamp,
  event_id,
  operation,
  data,
  old_data