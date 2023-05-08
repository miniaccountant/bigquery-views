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
  DUE,
  METHOD,
  BANKBENEFECIARYNAME,
  BANKACCOUNT,
  BANKNAME,
  BANKSWIFTCODE,
  INTERMEDIARYBANKNAME,
  INTERMEDIARYBANKSWIFTCODE,
  INTERMEDIARYBANKABA,
  ACHBENEFECIARYNAME,
  ACHROUTINGNUMBER,
  ACHACCOUNTNUMBER,
  ACHTYPE,
  ACHBANKADDRESS,
  PAYONEEREMAIL,
  PAYONEERACCOUNT,
  PAYPALEMAIL,
  PAYPALACCOUNT,
  PAYPALMOBILE,
  CARDNUMBER,
  CARDOWNER,
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
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.due')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS DUE,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.method')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS METHOD,
      FIRST_VALUE(
        JSON_EXTRACT_SCALAR(data, '$.bankBenefeciaryName')
      ) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS BANKBENEFECIARYNAME,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.bankAccount')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS BANKACCOUNT,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.bankName')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS BANKNAME,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.bankSwiftCode')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS BANKSWIFTCODE,
      FIRST_VALUE(
        JSON_EXTRACT_SCALAR(data, '$.intermediaryBankName')
      ) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS INTERMEDIARYBANKNAME,
      FIRST_VALUE(
        JSON_EXTRACT_SCALAR(data, '$.intermediaryBankSwiftCode')
      ) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS INTERMEDIARYBANKSWIFTCODE,
      FIRST_VALUE(
        JSON_EXTRACT_SCALAR(data, '$.intermediaryBankAba')
      ) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS INTERMEDIARYBANKABA,
      FIRST_VALUE(
        JSON_EXTRACT_SCALAR(data, '$.achBenefeciaryName')
      ) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS ACHBENEFECIARYNAME,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.achRoutingNumber')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS ACHROUTINGNUMBER,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.achAccountNumber')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS ACHACCOUNTNUMBER,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.achType')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS ACHTYPE,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.achBankAddress')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS ACHBANKADDRESS,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.payoneerEmail')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS PAYONEEREMAIL,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.payoneerAccount')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS PAYONEERACCOUNT,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.paypalEmail')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS PAYPALEMAIL,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.paypalAccount')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS PAYPALACCOUNT,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.paypalMobile')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS PAYPALMOBILE,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.cardNumber')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS CARDNUMBER,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.cardOwner')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS CARDOWNER,
      FIRST_VALUE(JSON_EXTRACT_SCALAR(data, '$.user')) OVER(
        PARTITION BY document_name
        ORDER BY
          timestamp DESC
      ) AS USER
    FROM
      `invoicemaker-f5e1d.firestore_export.paymentsSettings_raw_latest`
  )
WHERE
  NOT is_deleted
GROUP BY
  document_name,
  document_id,
  timestamp,
  operation,
  DUE,
  METHOD,
  BANKBENEFECIARYNAME,
  BANKACCOUNT,
  BANKNAME,
  BANKSWIFTCODE,
  INTERMEDIARYBANKNAME,
  INTERMEDIARYBANKSWIFTCODE,
  INTERMEDIARYBANKABA,
  ACHBENEFECIARYNAME,
  ACHROUTINGNUMBER,
  ACHACCOUNTNUMBER,
  ACHTYPE,
  ACHBANKADDRESS,
  PAYONEEREMAIL,
  PAYONEERACCOUNT,
  PAYPALEMAIL,
  PAYPALACCOUNT,
  PAYPALMOBILE,
  CARDNUMBER,
  CARDOWNER,
  USER