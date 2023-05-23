SELECT `users` AS `USER`,
  `users_settings` AS `USER_SETTINGS`,
  `payments_settings` AS `PAYMENT_SETTINGS`,
  `invoices`.`document_id` AS `ID`,
  CAST(`invoices`.`DATE` AS STRING FORMAT "YYYYMMDD") AS `DATE`,
  `invoices`.`NUMBER`,
  `invoices`.`CURRENCY` AS `CURRENCY`,
  `customers` AS `CUSTOMER`,
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
    SELECT AS STRUCT CAST(
        `invoicemaker-f5e1d`.`firestore_export`.`firestoreTimestamp`(JSON_EXTRACT(`transactions`, '$.date')) AS STRING FORMAT "YYYYMMDD"
      ) AS `DATE`,
      `invoicemaker-f5e1d`.`firestore_export`.`firestoreNumber`(JSON_EXTRACT(`transactions`, '$.sum')) AS `SUM`,
      `invoicemaker-f5e1d`.`firestore_export`.`firestoreNumber`(JSON_EXTRACT(`transactions`, '$.exchangeRate')) AS `EXCHANGE_RATE`
    FROM UNNEST(
        JSON_EXTRACT_ARRAY(`invoices`.`TRANSACTIONS`, '$')
      ) AS `transactions`
  ) AS `TRANSACTIONS`
FROM `invoicemaker-f5e1d.firestore_export.invoices_schema_Invoice_latest` AS `invoices`
  LEFT JOIN `invoicemaker-f5e1d.firestore_export.customers_schema_Customer_latest` AS `customers` ON `invoices`.`CUSTOMER` = CONCAT("customers/", `customers`.`document_id`)
  LEFT JOIN `invoicemaker-f5e1d.firestore_export.users_schema_User_latest` AS `users` ON `invoices`.`USER` = CONCAT("users/", `users`.`document_id`)
  LEFT JOIN `invoicemaker-f5e1d.firestore_export.usersSettings_schema_UserSettings_latest` AS `users_settings` ON `invoices`.`USER` = CONCAT("users/", `users_settings`.`document_id`)
  LEFT JOIN `invoicemaker-f5e1d.firestore_export.paymentsSettings_schema_PaymentSettings_latest` AS `payments_settings` ON `customers`.`paymentSettings` = CONCAT(
    "paymentsSettings/",
    `payments_settings`.`document_id`
  )
ORDER BY `invoices`.`NUMBER`