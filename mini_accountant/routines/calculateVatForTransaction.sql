CREATE OR REPLACE FUNCTION `invoicemaker-f5e1d.mini_accountant.calculateVatForTransaction`(transactionId INT64, vat FLOAT64, transactions ARRAY<STRUCT<DATETIME TIMESTAMP, SUM NUMERIC, EXCHANGE_RATE NUMERIC>>) RETURNS NUMERIC LANGUAGE js AS R"""
function calculateVatForTransaction(transactionId0, vat0, transactions0) {
  let vatRemains = vat0;
  for(let i = 0; i < transactions0.length; i++){
    const vat1 = (transactions0[i].SUM > vatRemains) ? vatRemains : transactions0[i].SUM;
    vatRemains = vatRemains - vat1;
    if(i == transactionId0) return vat1;
  }
}
return calculateVatForTransaction(transactionId, vat, transactions);
""";