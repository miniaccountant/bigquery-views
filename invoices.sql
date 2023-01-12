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
SELECT REPLACE(
        `invoicemaker-f5e1d.firestore_export.invoices_schema_Invoice_latest`.`USER`,
        'users/',
        ''
    ) AS `USER`,
    `invoicemaker-f5e1d.firestore_export.invoices_schema_Invoice_latest`.`document_id` AS `ID`,
    CAST(
        `invoicemaker-f5e1d.firestore_export.invoices_schema_Invoice_latest`.`DATE` AS STRING FORMAT "YYYYMMDD"
    ) AS `DATE`,
    `invoicemaker-f5e1d.firestore_export.invoices_schema_Invoice_latest`.`NUMBER`,
    `invoicemaker-f5e1d.firestore_export.customers_schema_Customer_latest`.`COMPANYNAME` AS `CUSTOMER`,
    ARRAY (
        SELECT AS STRUCT REPLACE(
                JSON_EXTRACT_SCALAR(`positions`, '$.service'),
                'services/',
                ''
            ) AS `ID`,
            JSON_EXTRACT_SCALAR(`positions`, '$.data.name') AS `NAME`,
            JSON_EXTRACT_SCALAR(`positions`, '$.data.currency') AS `CURRENCY`,
            `invoicemaker-f5e1d`.`firestore_export`.`firestoreNumber`(JSON_EXTRACT(`positions`, '$.data.price')) AS `PRICE`
        FROM UNNEST(
                JSON_EXTRACT_ARRAY(
                    `invoicemaker-f5e1d.firestore_export.invoices_schema_Invoice_latest`.`POSITIONS`,
                    '$'
                )
            ) AS `positions`
    ) AS `POSITIONS`,
    ARRAY (
        SELECT AS STRUCT CAST(
                `invoicemaker-f5e1d`.`firestore_export`.`firestoreTimestamp`(JSON_EXTRACT(`transactions`, '$.date')) AS STRING FORMAT "YYYYMMDD"
            ) AS `DATE`,
            `invoicemaker-f5e1d`.`firestore_export`.`firestoreNumber`(JSON_EXTRACT(`transactions`, '$.sum')) AS `SUM`,
            `invoicemaker-f5e1d`.`firestore_export`.`firestoreNumber`(JSON_EXTRACT(`transactions`, '$.exchangeRate')) AS `EXCHANGE_RATE`
        FROM UNNEST(
                JSON_EXTRACT_ARRAY(
                    `invoicemaker-f5e1d.firestore_export.invoices_schema_Invoice_latest`.`TRANSACTIONS`,
                    '$'
                )
            ) AS `transactions`
    ) AS `TRANSACTIONS`
FROM `invoicemaker-f5e1d.firestore_export.invoices_schema_Invoice_latest`
    INNER JOIN `invoicemaker-f5e1d.firestore_export.customers_schema_Customer_latest` ON `invoicemaker-f5e1d.firestore_export.invoices_schema_Invoice_latest`.`CUSTOMER` = CONCAT(
        "customers/",
        `invoicemaker-f5e1d.firestore_export.customers_schema_Customer_latest`.`document_id`
    )
    INNER JOIN `access` ON `access`.`USER_ID` = REPLACE(
        `invoicemaker-f5e1d.firestore_export.invoices_schema_Invoice_latest`.`USER`,
        'users/',
        ''
    )
WHERE `access`.`USER_ID` = REPLACE(
        `invoicemaker-f5e1d.firestore_export.invoices_schema_Invoice_latest`.`USER`,
        'users/',
        ''
    )
ORDER BY `NUMBER`