CREATE OR REPLACE TABLE FUNCTION `invoicemaker-f5e1d.mini_accountant.transactions_slice_by_range`(
    fromDate TIMESTAMP,
    toDate TIMESTAMP,
    tzid STRING
  ) AS (
    WITH `filtered_invoices` AS (
      SELECT *
      FROM `invoicemaker-f5e1d.mini_accountant.invoices` AS `invoice`
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
    SELECT `filtered_invoices`.`USER_CURRENCY` AS `userCurrency`,
      `filtered_invoices`.`USER_TAXFEE` AS `userTaxFee`,
      `filtered_invoices`.`ID` AS `invoiceId`,
      DATE(`filtered_invoices`.`DATETIME`, tzid) AS `invoiceDate`,
      `filtered_invoices`.`NUMBER` AS `invoiceNumber`,
      (
        SELECT SUM(
            `position`.`PRICE` * (100 + COALESCE(`position`.`VAT`, 0) / 100)
          )
        FROM UNNEST(`filtered_invoices`.`POSITIONS`) AS `position`
      ) AS `invoiceAmount`,
      `filtered_invoices`.`CUSTOMER` AS `customer`,
      `filtered_transacions`.`ID` AS `transactionId`,
      DATE(`filtered_transacions`.`DATETIME`, tzid) AS `transactionDate`,
      `filtered_transacions`.`SUM` AS `transactionSum`,
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