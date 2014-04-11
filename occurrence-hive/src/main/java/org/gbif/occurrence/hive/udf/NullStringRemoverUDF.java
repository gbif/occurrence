package org.gbif.occurrence.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * A simple UDF for Hive to convert null to an empty string, in order to avoid an ugly /N in the output.
 */
@Description(
  name = "cleanNullString",
  value = "_FUNC_(field)")
public class NullStringRemoverUDF extends UDF {

  private final Text text = new Text();

  public Text evaluate(Text field) {
    if (field == null) {
      text.set("");
    } else {
      text.set(field);
    }

    return text;
  }
}
