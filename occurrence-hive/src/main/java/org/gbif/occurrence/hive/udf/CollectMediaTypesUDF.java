package org.gbif.occurrence.hive.udf;

import org.gbif.api.model.common.MediaObject;
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
    List<MediaObject> medias = MediaSerDeserUtils.fromJson(jsonMedias);
    List<String> mediaTypes = Lists.newArrayList();
    if (medias != null && !medias.isEmpty()) {
      for (MediaObject mediaObject : medias) {
        if (mediaObject.getType() != null) {
          mediaTypes.add(mediaObject.getType().name().toUpperCase());
        }
      }
    }
    return mediaTypes;
  }

}
