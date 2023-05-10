WITH `filtered_invoices` AS (
  SELECT *
  FROM `invoicemaker-f5e1d.mini_accountant_admin.invoices_v2` AS `invoice`
  WHERE `invoice`.`DATETIME` >= fromDate
    AND `invoice`.`DATETIME` <= toDate
  ORDER BY `invoice`.`NUMBER`
)
SELECT `filtered_invoices`.`ID` AS `invoiceId`,
  `filtered_invoices`.`USER` AS `user`,
  `filtered_invoices`.`USER_SETTINGS` AS `user_settings`,
  `filtered_invoices`.`PAYMENT_SETTINGS` AS `payment_settings`,
  DATE(`filtered_invoices`.`DATETIME`, tzid) AS `invoiceDate`,
  `filtered_invoices`.`NUMBER` AS `invoiceNumber`,
  (
    SELECT SUM(`position`.`PRICE`)
    FROM UNNEST(`filtered_invoices`.`POSITIONS`) AS `position`
  ) AS `invoiceAmount`,
  `filtered_invoices`.`CUSTOMER` AS `customer`,
  (
    SELECT SUM(`transaction`.`SUM`)
    FROM UNNEST(`filtered_invoices`.`TRANSACTIONS`) AS `transaction`
  ) AS `paidAmount`,
  (
    SELECT SUM(
        `transaction`.`SUM` * `transaction`.`EXCHANGE_RATE`
      )
    FROM UNNEST(`filtered_invoices`.`TRANSACTIONS`) AS `transaction`
  ) AS `paidAmountInCurr`,
  `filtered_invoices`.`CURRENCY` AS `invoiceCurrency`
FROM `filtered_invoices`