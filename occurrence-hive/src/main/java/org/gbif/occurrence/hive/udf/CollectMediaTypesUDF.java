/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.hive.udf;

import org.gbif.occurrence.common.json.MediaSerDeserUtils;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.google.common.collect.Lists;

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
    return null;
  }

  /**
   * Deserialize and extract the media types.
   */
  private static List<String> selectMediaTypes(String jsonMedias) {
    List<String> result = Lists.newArrayList(MediaSerDeserUtils.extractMediaTypes(jsonMedias));
    return result.isEmpty() ? null : result;
  }

}
