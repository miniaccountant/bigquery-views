# BigQuery Views

## view for `mini_accountant_admin` (private)

- `invoices`

## routines for `mini_accountant_admin`

- `calculateVatForTransaction`
- `invoices_slice_by_range_v2`
- `transactions_slice_by_range_v2`

## view for `mini_accountant` (public)

- `invoices`

## routines for `mini_accountant`

- `calculateVatForTransaction`
- `invoices_slice_by_range`
- `positions_slice_by_range`
- `transactions_slice_by_range`

## example of create routine (table function)

```sql
CREATE OR REPLACE TABLE FUNCTION `invoicemaker-f5e1d.mini_accountant.transactions_slice_by_range`(
    fromDate DATE, 
    toDate DATE
) AS (
    -- the code of the table function
)
```

## example of the call routine from Google Looker

```sql
SELECT * FROM `invoicemaker-f5e1d.mini_accountant.transactions_slice_by_range`(
    PARSE_DATE("%Y%m%d",@DS_START_DATE), 
    PARSE_DATE("%Y%m%d", @DS_END_DATE)
);
```
