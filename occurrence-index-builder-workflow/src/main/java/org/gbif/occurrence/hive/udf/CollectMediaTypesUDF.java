package org.gbif.occurrence.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author fede
 */
@Description(
  name = "collectMediaTypes",
  value = "_FUNC_(field)")
public class CollectMediaTypesUDF extends UDF {

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
