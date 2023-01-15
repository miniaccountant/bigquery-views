# BigQuery Views

## view for `firestore_export` (private)

- `mini_accountant_invoices`

## view for `mini_accountant` (public)

- `invoices`
- `users`

## routines for `mini_accountant`

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
