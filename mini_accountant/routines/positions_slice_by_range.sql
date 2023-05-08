WITH `filtered_invoices` AS (
    SELECT * FROM `invoicemaker-f5e1d.mini_accountant.invoices` AS `invoice`
    WHERE `invoice`.`DATE` >= CAST(fromDate AS STRING  FORMAT "YYYYMMDD") 
      AND `invoice`.`DATE` <= CAST(toDate AS STRING  FORMAT "YYYYMMDD")
    ORDER BY `invoice`.`NUMBER`
  )
  SELECT
    `filtered_invoices`.`USER_CURRENCY` AS `userCurrency`,
    `filtered_invoices`.`USER_TAXFEE` AS `userTaxFee`,
    `filtered_invoices`.`ID` AS `invoiceId`, 
    PARSE_DATE ("%Y%m%d", `filtered_invoices`.`DATE`) AS `invoiceDate`, 
    `filtered_invoices`.`NUMBER` AS `invoiceNumber`,
    (SELECT SUM(`position`.`PRICE`)
      FROM UNNEST(`filtered_invoices`.`POSITIONS`) AS `position`
    ) AS `invoiceAmount`, 
    `filtered_invoices`.`CUSTOMER` AS `customer`,
    `filtered_positions`.`ID` AS `positionId`,
    `filtered_positions`.`NAME` AS `positionName`,
    `filtered_positions`.`PRICE` AS `positionPrice`,
    `filtered_positions`.`CURRENCY` AS `positionCurrency`
  FROM `filtered_invoices`
  CROSS JOIN UNNEST(
    `filtered_invoices`.`POSITIONS`
  ) AS `filtered_positions`