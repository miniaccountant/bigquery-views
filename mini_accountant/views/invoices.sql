WITH `access` AS (
  SELECT `document_id` AS `USER_ID`
  FROM `invoicemaker-f5e1d.firestore_export.users_raw_changelog`
  WHERE SESSION_USER() =(
      JSON_EXTRACT_SCALAR(
        `invoicemaker-f5e1d.firestore_export.users_raw_changelog`.`data`,
        '$.email'
      )
    )
  ORDER BY `timestamp` DESC
  LIMIT 1
)
SELECT REPLACE(`invoices`.`USER`, 'users/', '') AS `USER`,
  `users_settings`.`CURRENCY` AS `USER_CURRENCY`,
  `users_settings`.`TAXFEE` AS `USER_TAXFEE`,
  `invoices`.`document_id` AS `ID`,
  `invoices`.`DATE` AS `DATETIME`,
  `invoices`.`NUMBER`,
  `customers`.`COMPANYNAME` AS `CUSTOMER`,
  ARRAY (
    SELECT AS STRUCT REPLACE(
        JSON_EXTRACT_SCALAR(`positions`, '$.service'),
        'services/',
        ''
      ) AS `ID`,
      JSON_EXTRACT_SCALAR(`positions`, '$.data.name') AS `NAME`,
      JSON_EXTRACT_SCALAR(`positions`, '$.data.currency') AS `CURRENCY`,
      `invoicemaker-f5e1d`.`firestore_export`.`firestoreNumber`(JSON_EXTRACT(`positions`, '$.data.price')) AS `PRICE`
    FROM UNNEST(JSON_EXTRACT_ARRAY(`invoices`.`POSITIONS`, '$')) AS `positions`
  ) AS `POSITIONS`,
  ARRAY (
    SELECT AS STRUCT `invoicemaker-f5e1d`.`firestore_export`.`firestoreTimestamp`(JSON_EXTRACT(`transactions`, '$.date')) AS `DATETIME`,
      `invoicemaker-f5e1d`.`firestore_export`.`firestoreNumber`(JSON_EXTRACT(`transactions`, '$.sum')) AS `SUM`,
      `invoicemaker-f5e1d`.`firestore_export`.`firestoreNumber`(JSON_EXTRACT(`transactions`, '$.exchangeRate')) AS `EXCHANGE_RATE`
    FROM UNNEST(
        JSON_EXTRACT_ARRAY(`invoices`.`TRANSACTIONS`, '$')
      ) AS `transactions`
  ) AS `TRANSACTIONS`
FROM `invoicemaker-f5e1d.firestore_export.invoices_schema_Invoice_latest` AS `invoices`
  LEFT JOIN `invoicemaker-f5e1d.firestore_export.customers_schema_Customer_latest` AS `customers` ON `invoices`.`CUSTOMER` = CONCAT("customers/", `customers`.`document_id`)
  LEFT JOIN `invoicemaker-f5e1d.firestore_export.usersSettings_schema_UserSettings_latest` AS `users_settings` ON `invoices`.`USER` = CONCAT("users/", `users_settings`.`document_id`)
  LEFT JOIN `access` ON `access`.`USER_ID` = REPLACE(`invoices`.`USER`, 'users/', '')
WHERE `access`.`USER_ID` = REPLACE(`invoices`.`USER`, 'users/', '')
ORDER BY `NUMBER`