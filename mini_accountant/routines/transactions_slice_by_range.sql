(
    (
        (
            (
                WITH `filtered_invoices` AS (
                    SELECT *
                    FROM `invoicemaker-f5e1d.mini_accountant.invoices` AS `invoice`
                    WHERE (
                            SELECT ARRAY_LENGTH(
                                    ARRAY(
                                        SELECT `transaction`.`DATE`
                                        FROM UNNEST(`invoice`.`TRANSACTIONS`) AS `transaction`
                                        WHERE `transaction`.`DATE` >= CAST(fromDate AS STRING FORMAT "YYYYMMDD")
                                            AND `transaction`.`DATE` <= CAST(toDate AS STRING FORMAT "YYYYMMDD")
                                    )
                                )
                        ) != 0
                    ORDER BY `invoice`.`NUMBER`
                )
                SELECT `filtered_invoices`.`ID` AS `invoiceId`,
                    PARSE_DATE ("%Y%m%d", `filtered_invoices`.`DATE`) AS `invoiceDate`,
                    `filtered_invoices`.`NUMBER` AS `invoiceNumber`,
                    (
                        SELECT SUM(`position`.`PRICE`)
                        FROM UNNEST(`filtered_invoices`.`POSITIONS`) AS `position`
                    ) AS `invoiceAmount`,
                    `filtered_invoices`.`CUSTOMER` AS `customer`,
                    `filtered_transacions`.`ID` AS `transactionId`,
                    PARSE_DATE ("%Y%m%d", `filtered_transacions`.`DATE`) AS `transactionDate`,
                    `filtered_transacions`.`SUM` AS `transactionSum`,
                    `filtered_transacions`.`EXCHANGE_RATE` AS `transactionExchangeRate`
                FROM `filtered_invoices`
                    CROSS JOIN UNNEST(
                        ARRAY(
                            SELECT AS STRUCT *
                            FROM `filtered_invoices`.`TRANSACTIONS` AS `transaction` WITH OFFSET AS `ID`
                            WHERE `transaction`.`DATE` >= CAST(fromDate AS STRING FORMAT "YYYYMMDD")
                                AND `transaction`.`DATE` <= CAST(toDate AS STRING FORMAT "YYYYMMDD")
                            ORDER BY `ID`
                        )
                    ) AS `filtered_transacions`
            )
        )
    )
)