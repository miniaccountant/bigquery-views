CREATE OR REPLACE FUNCTION `invoicemaker-f5e1d.mini_accountant_admin.calculateVatForTransaction`(
    transactionId INT64,
    vat NUMERIC,
    transactions ARRAY < STRUCT < DATETIME TIMESTAMP,
    SUM NUMERIC,
    EXCHANGE_RATE NUMERIC >>
  ) RETURNS FLOAT64 LANGUAGE js AS R """
function calculateVatForTransaction(transactionId, vat, transactions) {
  let vatRemains = vat;
  for(let i = 0; i < transactions.length; i++){
    const vat = (transactions[i].SUM > vatRemains) ? vatRemains : transactions[i].SUM;
    vatRemains = vatRemains - vat;
    if(i == transactionId) return vat;
  }
}
return calculateVatForTransaction(transactionId, vat, transactions);
""";