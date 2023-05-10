SELECT document_name,
  document_id,
  timestamp,
  operation,
  JSON_EXTRACT_SCALAR(data, '$.name') AS NAME,
  `invoicemaker-f5e1d.firestore_export.firestoreNumber`(JSON_EXTRACT_SCALAR(data, '$.price')) AS PRICE,
  JSON_EXTRACT_SCALAR(data, '$.currency') AS CURRENCY,
  JSON_EXTRACT_SCALAR(data, '$.user') AS USER
FROM `invoicemaker-f5e1d.firestore_export.services_raw_changelog`