SELECT document_name,
  document_id,
  timestamp,
  operation,
  `invoicemaker-f5e1d.firestore_export.firestoreTimestamp`(JSON_EXTRACT(data, '$.date')) AS DATE,
  JSON_EXTRACT_SCALAR(data, '$.name') AS NAME,
  JSON_EXTRACT_SCALAR(data, '$.description') AS DESCRIPTION,
  JSON_EXTRACT_SCALAR(data, '$.currency') AS CURRENCY,
  `invoicemaker-f5e1d.firestore_export.firestoreNumber`(JSON_EXTRACT_SCALAR(data, '$.sum')) AS SUM,
  `invoicemaker-f5e1d.firestore_export.firestoreNumber`(JSON_EXTRACT_SCALAR(data, '$.exchangeRate')) AS EXCHANGE_RATE,
  `invoicemaker-f5e1d.firestore_export.firestoreNumber`(JSON_EXTRACT_SCALAR(data, '$.vat')) AS VAT,
  JSON_EXTRACT_SCALAR(data, '$.user') AS USER
FROM `invoicemaker-f5e1d.firestore_export.expenses_raw_changelog`