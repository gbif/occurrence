package org.gbif.occurrence.hive.udf;

import org.gbif.occurrence.common.json.MediaSerDeserUtils;

import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Deserialize a JSON string into a List<MediaObject> and extracts the media types of each object.
 */
@Description(
  name = "collectMediaTypes",
  value = "_FUNC_(field)")
public class CollectMediaTypesUDF extends UDF {

  public List<String> evaluate(Text field) {
    if (field != null) {
      return selectMediaTypes(field.toString());
    }
    return Lists.newArrayList();
  }

  /**
   * Deserialize and extract the media types.
   */
  private List<String> selectMediaTypes(String jsonMedias) {
    return Lists.newArrayList(MediaSerDeserUtils.extractMediaTypes(jsonMedias));
  }

}
