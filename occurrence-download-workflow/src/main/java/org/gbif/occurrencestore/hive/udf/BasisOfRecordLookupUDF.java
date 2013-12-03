package org.gbif.occurrencestore.hive.udf;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.occurrencestore.util.BasisOfRecordConverter;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Converts the basis of record int stored in hbase into the textual BasisOfRecord enum value.
 */
@Description(
  name = "getBorEnum",
  value = "_FUNC_(borAsInt)")
public class BasisOfRecordLookupUDF extends UDF {

  private static final BasisOfRecordConverter BOR_CONVERTER = new BasisOfRecordConverter();
  private static final Text BOR = new Text();

  public Text evaluate(Text borAsInt) {
    // default to nothing in case anything goes "wrong" below
    BOR.set("");

    if (borAsInt != null && borAsInt.getLength() > 0) {
      try {
        Integer code = Integer.parseInt(borAsInt.toString());
        if (code != null) {
          BasisOfRecord bor = BOR_CONVERTER.toEnum(code);
          if (bor != null) {
            BOR.set(bor.name());
          } else {
            System.err.println("Unknown BasisOfRecord code: " + borAsInt.toString());
          }
        }
      } catch (NumberFormatException e) {
        // swallow it all and log
        System.err.println("Non numerical representation of BasisOfRecord: " + borAsInt.toString());
      }
    }

    return BOR;
  }

}
