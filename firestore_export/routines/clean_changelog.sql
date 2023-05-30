CREATE OR REPLACE PROCEDURE `invoicemaker-f5e1d.firestore_export.clean_changelog`(tableName STRING) BEGIN
DECLARE query STRING;
SET query = CONCAT(
    "DELETE FROM `invoicemaker-f5e1d.firestore_export.",
    tableName,
    "` ",
    "\n",
    "WHERE (document_name, timestamp) IN",
    "\n",
    "(",
    "\n",
    "  WITH latest AS (",
    "\n",
    "    SELECT MAX(timestamp) as timestamp, document_name",
    "\n",
    "    FROM `invoicemaker-f5e1d.firestore_export.",
    tableName,
    "`",
    "\n",
    "    GROUP BY document_name",
    "\n",
    "  )",
    "\n",
    "  SELECT (t.document_name, t.timestamp)",
    "\n",
    "  FROM `invoicemaker-f5e1d.firestore_export.",
    tableName,
    "` AS t",
    "\n",
    "  JOIN latest  ON (t.document_name = latest.document_name )",
    "\n",
    "  WHERE t.timestamp != latest.timestamp",
    "\n",
    "  AND DATETIME(t.timestamp) < DATE_ADD(CURRENT_DATETIME(), INTERVAL -1 MONTH)",
    "\n",
    ");"
  );
EXECUTE IMMEDIATE query;
END;