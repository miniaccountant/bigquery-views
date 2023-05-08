SELECT
  document_name,
  document_id,
  timestamp,
  operation,
  JSON_EXTRACT_SCALAR(data, '$.companyName') AS COMPANYNAME,
  JSON_EXTRACT_SCALAR(data, '$.country') AS COUNTRY,
  JSON_EXTRACT_SCALAR(data, '$.state') AS STATE,
  JSON_EXTRACT_SCALAR(data, '$.addressLine1') AS ADDRESSLINE1,
  JSON_EXTRACT_SCALAR(data, '$.addressLine2') AS ADDRESSLINE2,
  JSON_EXTRACT_SCALAR(data, '$.currency') AS CURRENCY,
  JSON_EXTRACT_SCALAR(data, '$.invoiceTemplate') AS INVOICETEMPLATE,
  JSON_EXTRACT_SCALAR(data, '$.paymentSettings') AS PAYMENTSETTINGS,
  JSON_EXTRACT_SCALAR(data, '$.user') AS USER
FROM
  `invoicemaker-f5e1d.firestore_export.customers_raw_changelog`