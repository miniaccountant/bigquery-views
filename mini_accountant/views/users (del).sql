SELECT *
FROM `invoicemaker-f5e1d.firestore_export.users_schema_User_latest`
WHERE SESSION_USER() = `invoicemaker-f5e1d.firestore_export.users_schema_User_latest`.`EMAIL`