CREATE OR REPLACE TABLE FUNCTION `invoicemaker-f5e1d.mini_accountant_admin.transactions_slice_by_range_v2`(
    fromDate TIMESTAMP,
    toDate TIMESTAMP,
    tzid STRING
  ) AS (
    WITH `filtered_invoices` AS (
      SELECT *
      FROM `invoicemaker-f5e1d.mini_accountant_admin.invoices_v2` AS `invoice`
      WHERE (
          SELECT ARRAY_LENGTH(
              ARRAY(
                SELECT `transaction`.`DATETIME`
                FROM UNNEST(`invoice`.`TRANSACTIONS`) AS `transaction`
                WHERE `transaction`.`DATETIME` >= fromDate
                  AND `transaction`.`DATETIME` <= toDate
              )
            )
        ) != 0
      ORDER BY `invoice`.`NUMBER`
    )
    SELECT `filtered_invoices`.`ID` AS `invoiceId`,
      DATE(`filtered_invoices`.`DATETIME`, tzid) AS `invoiceDate`,
      `filtered_invoices`.`NUMBER` AS `invoiceNumber`,
      (
        SELECT SUM(
            `position`.`PRICE` * COALESCE(`position`.`QUANTITY`, 1)
          )
        FROM UNNEST(`filtered_invoices`.`POSITIONS`) AS `position`
      ) AS `invoiceAmountRaw`,
      (
        SELECT SUM(
            `position`.`PRICE` * COALESCE(`position`.`QUANTITY`, 1) * (100 + COALESCE(`position`.`VAT`) / 100)
          )
        FROM UNNEST(`filtered_invoices`.`POSITIONS`) AS `position`
      ) AS `invoiceAmount`,
      (
        SELECT SUM(
            `position`.`PRICE` * COALESCE(`position`.`QUANTITY`, 1) * (COALESCE(`position`.`VAT`) / 100)
          )
        FROM UNNEST(`filtered_invoices`.`POSITIONS`) AS `position`
      ) AS `invoiceVat`,
      `filtered_invoices`.`CURRENCY` AS `invoiceCurrency`,
      `filtered_invoices`.`USER` AS `user`,
      `filtered_invoices`.`PAYMENT_SETTINGS` AS `paymentSettings`,
      `filtered_invoices`.`CUSTOMER` AS `customer`,
      `filtered_transacions`.`ID` AS `transactionId`,
      DATE(`filtered_transacions`.`DATETIME`, tzid) AS `transactionDate`,
      `filtered_transacions`.`SUM` AS `transactionSum`,
      `invoicemaker-f5e1d.mini_accountant_admin.calculateVatForTransaction`(
        `filtered_transacions`.`ID`,
        (
          SELECT SUM(
              `position`.`PRICE` * COALESCE(`position`.`QUANTITY`, 1) * (COALESCE(`position`.`VAT`, 0) / 100)
            )
          FROM UNNEST(`filtered_invoices`.`POSITIONS`) AS `position`
        ),
        `filtered_invoices`.`TRANSACTIONS`
      ) AS `transactionVat`,
      `filtered_transacions`.`EXCHANGE_RATE` AS `transactionExchangeRate`
    FROM `filtered_invoices`
      CROSS JOIN UNNEST(
        ARRAY(
          SELECT AS STRUCT *
          FROM `filtered_invoices`.`TRANSACTIONS` AS `transaction` WITH OFFSET AS `ID`
          WHERE `transaction`.`DATETIME` >= fromDate
            AND `transaction`.`DATETIME` <= toDate
          ORDER BY `ID`
        )
      ) AS `filtered_transacions`
  );