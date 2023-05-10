SELECT document_name,
  document_id,
  timestamp,
  operation,
  `invoicemaker-f5e1d.firestore_export.firestoreNumber`(JSON_EXTRACT_SCALAR(data, '$.number')) AS NUMBER,
  `invoicemaker-f5e1d.firestore_export.firestoreTimestamp`(JSON_EXTRACT(data, '$.date')) AS DATE,
  JSON_EXTRACT(data, '$.positions') AS POSITIONS,
  JSON_EXTRACT(data, '$.transactions') AS TRANSACTIONS,
  JSON_EXTRACT_SCALAR(data, '$.customer') AS CUSTOMER,
  JSON_EXTRACT_SCALAR(data, '$.currency') AS CURRENCY,
  JSON_EXTRACT_SCALAR(data, '$.user') AS USER
FROM `invoicemaker-f5e1d.firestore_export.invoices_raw_changelog`