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
package org.gbif.occurrence.download.hive;

import org.gbif.api.model.occurrence.Download;

import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;

import freemarker.template.Configuration;
import freemarker.template.Template;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

/**
 * Generates a query file to be used to query the requested extensions of a download.
 */
@Data
@Builder
public class ExtensionsQuery {

  private final BufferedWriter writer;

  private static final String TEMPLATE_FILE = "templates/download/execute-extensions-query.ftl";

  /**
   * Reads the template file to a string.
   */
  @SneakyThrows
  private String readTemplateFileToString() {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(TEMPLATE_FILE)) {
      StringWriter writer = new StringWriter();
      IOUtils.copy(is, writer, StandardCharsets.UTF_8);
      return writer.toString();
    }
  }

  /**
   * Loads the freemarker template.
   */
  @SneakyThrows
  private Template template() {
    return new Template("extensions",
                        new StringReader(readTemplateFileToString()),
                        new Configuration(Configuration.getVersion()));
  }

  /**
   * Sets the template variable from a Download object.
   */
  private Map<String,Object> templateVariables(Download download) {
    HashMap<String,Object> variables = new HashMap<>();

    variables.put("extensions", download.getRequest().getExtensions().stream().map(OccurrenceHDFSTableDefinition.ExtensionTable::new)
                                  .collect(Collectors.toList()));
    variables.put("downloadTableName", download.getKey());
    variables.put("interpretedTable", download.getKey() + "_interpreted");
    variables.put("tableName", download.getRequest().getType().toString().toLowerCase());
    return variables;
  }

  /**
   * Generates the Hive query file querying extensions to the supplied writer.
   */
  @SneakyThrows
  public void generateExtensionsQueryHQL(Download download) {
    template().process(templateVariables(download), writer);
  }

}
