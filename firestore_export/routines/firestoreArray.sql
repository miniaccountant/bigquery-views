CREATE OR REPLACE FUNCTION `invoicemaker-f5e1d.firestore_export.firestoreArray`(json STRING) RETURNS ARRAY<STRING> LANGUAGE js AS R"""
function getArray(json) {
      if(json) {
        const parsed = JSON.parse(json);
        
        if (Array.isArray(parsed)) {
          return parsed.map((x) => {
            if (typeof x === 'string') {
              return x;
            } else {
              return JSON.stringify(x);
            }
          });
        }
        
        return [];
      }

      return [];
    }
    
    return getArray(json);
""";