CREATE OR REPLACE PROCEDURE `invoicemaker-f5e1d.firestore_export.clean_changelog_all`(tableNames ARRAY < STRING >) BEGIN FOR t IN (
    SELECT *
    FROM UNNEST(tableNames) AS name
  ) DO CALL `invoicemaker-f5e1d.firestore_export.clean_changelog`(t.name);
END FOR;
END;