SELECT document_name,
  document_id,
  timestamp,
  operation,
  JSON_EXTRACT_SCALAR(data, '$.type') AS TYPE,
  JSON_EXTRACT_SCALAR(data, '$.description') AS DESCRIPTION,
  JSON_EXTRACT(data, '$.paymentInfo') AS PAYMENT_INFO,
  `invoicemaker-f5e1d.firestore_export.firestoreTimestamp`(JSON_EXTRACT(data, '$.validTo')) AS VALID_TO,
  JSON_EXTRACT_SCALAR(data, '$.user') AS USER
FROM `invoicemaker-f5e1d.firestore_export.subscriptions_raw_changelog`