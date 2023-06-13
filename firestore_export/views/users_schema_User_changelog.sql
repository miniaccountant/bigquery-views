SELECT document_name,
  document_id,
  timestamp,
  operation,
  JSON_EXTRACT_SCALAR(data, '$.email') AS EMAIL,
  `invoicemaker-f5e1d.firestore_export.firestoreNumber`(JSON_EXTRACT_SCALAR(data, '$.invoiceMtx')) AS INVOICEMTX,
  JSON_EXTRACT_SCALAR(data, '$.fcmToken') AS FCMTOKEN,
  JSON_EXTRACT_SCALAR(data, '$.theme') AS THEME,
  JSON_EXTRACT_SCALAR(data, '$.customExchangeServer') AS CUSTOM_EXCHANGE_SERVER
FROM `invoicemaker-f5e1d.firestore_export.users_raw_changelog`