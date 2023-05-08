-- Given a user-defined schema over a raw JSON changelog, returns the
-- schema elements of the latest set of live documents in the collection.
--   timestamp: The Firestore timestamp at which the event took place.
--   operation: One of INSERT, UPDATE, DELETE, IMPORT.
--   event_id: The event that wrote this row.
--   <schema-fields>: This can be one, many, or no typed-columns
--                    corresponding to fields defined in the schema.
SELECT
  document_name,
  document_id,
  timestamp,
  operation,
  COMPANYNAME,
  IDNUMBER,
  EMAIL,
  COUNTRY,
  CITY,
  ADDRESSLINE1,
  ADDRESSLINE2,
  CURRENCY,
  TAXFEE,
  USER
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
      FIRST_VALUE(operation) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS operation,
      FIRST_VALUE(operation) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) = "DELETE" AS is_deleted,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.companyName')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS COMPANYNAME,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.idNumber')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS IDNUMBER,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.email')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS EMAIL,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.country')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS COUNTRY,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.city')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS CITY,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.addressLine1')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS ADDRESSLINE1,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.addressLine2')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS ADDRESSLINE2,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.currency')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS CURRENCY,
      `invoicemaker-f5e1d.firestore_export.firestoreNumber`(
        FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.taxFee')) OVER(
          PARTITION BY document_name
          ORDER BY
            timestamp DESC
        )
      ) AS TAXFEE,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.user')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS USER
    FROM
      `invoicemaker-f5e1d.firestore_export.usersSettings_raw_latest`
  )
WHERE
  NOT is_deleted
GROUP BY
  document_name,
  document_id,
  timestamp,
  operation,
  COMPANYNAME,
  IDNUMBER,
  EMAIL,
  COUNTRY,
  CITY,
  ADDRESSLINE1,
  ADDRESSLINE2,
  CURRENCY,
  TAXFEE,
  USER