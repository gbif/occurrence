package org.gbif.occurrence.hive.udf;

import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.common.parsers.BasisOfRecordParser;
import org.gbif.common.parsers.core.ParseResult;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Parses raw basis of record into a textual version.
 */
@Description(
  name = "parseBoR",
  value = "_FUNC_(bor)")
public class BasisOfRecordParseUDF extends UDF {

  private static final BasisOfRecordParser BOR_PARSER = BasisOfRecordParser.getInstance();
  private static final Text BOR = new Text();

  public Text evaluate(Text basisOfRecord) {
    if (basisOfRecord != null) {
      ParseResult<BasisOfRecord> parsed = BOR_PARSER.parse(basisOfRecord.toString());
      if (parsed.isSuccessful()) {
        BOR.set(parsed.getPayload().toString());
      }
    }

    return BOR;
  }
}
