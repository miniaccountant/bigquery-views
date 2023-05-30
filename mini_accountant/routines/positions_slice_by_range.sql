CREATE OR REPLACE TABLE FUNCTION `invoicemaker-f5e1d.mini_accountant.positions_slice_by_range`(
    fromDate TIMESTAMP,
    toDate TIMESTAMP,
    tzid STRING
  ) AS (
    WITH `filtered_invoices` AS (
      SELECT *
      FROM `invoicemaker-f5e1d.mini_accountant.invoices` AS `invoice`
      WHERE `invoice`.`DATETIME` >= fromDate
        AND `invoice`.`DATETIME` <= toDate
      ORDER BY `invoice`.`NUMBER`
    )
    SELECT `filtered_invoices`.`USER_CURRENCY` AS `userCurrency`,
      `filtered_invoices`.`USER_TAXFEE` AS `userTaxFee`,
      `filtered_invoices`.`ID` AS `invoiceId`,
      DATE(`filtered_invoices`.`DATETIME`, tzid) AS `invoiceDate`,
      `filtered_invoices`.`NUMBER` AS `invoiceNumber`,
      (
        SELECT SUM(`position`.`PRICE`)
        FROM UNNEST(`filtered_invoices`.`POSITIONS`) AS `position`
      ) AS `invoiceAmount`,
      `filtered_invoices`.`CUSTOMER` AS `customer`,
      `filtered_positions`.`ID` AS `positionId`,
      `filtered_positions`.`NAME` AS `positionName`,
      `filtered_positions`.`PRICE` AS `positionPrice`,
      `filtered_positions`.`CURRENCY` AS `positionCurrency`
    FROM `filtered_invoices`
      CROSS JOIN UNNEST(`filtered_invoices`.`POSITIONS`) AS `filtered_positions`
  );