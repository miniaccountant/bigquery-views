SELECT document_name,
  document_id,
  timestamp,
  operation,
  JSON_EXTRACT_SCALAR(data, '$.email') AS EMAIL,
  `invoicemaker-f5e1d.firestore_export.firestoreNumber`(JSON_EXTRACT_SCALAR(data, '$.invoiceMtx')) AS INVOICEMTX,
  JSON_EXTRACT_SCALAR(data, '$.fcmToken') AS FCMTOKEN
FROM `invoicemaker-f5e1d.firestore_export.users_raw_changelog`