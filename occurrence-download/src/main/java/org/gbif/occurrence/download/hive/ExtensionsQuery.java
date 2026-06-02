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

import java.util.List;
import java.util.Set;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.vocabulary.Extension;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.util.DownloadRequestUtils;

import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;

import freemarker.template.Configuration;
import freemarker.template.Template;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

/** Generates a query file to be used to query the requested extensions of a download. */
@Data
@Builder
public class ExtensionsQuery {

  private final Writer writer;

  private static final String TEMPLATE_FILE = "templates/download/execute-extensions-query.ftl";

  /** Reads the template file to a string. */
  @SneakyThrows
  private String readTemplateFileToString() {
    try (InputStream is = getClass().getClassLoader().getResourceAsStream(TEMPLATE_FILE)) {
      StringWriter writer = new StringWriter();
      IOUtils.copy(is, writer, StandardCharsets.UTF_8);
      return writer.toString();
    }
  }

  /** Loads the freemarker template. */
  @SneakyThrows
  private Template template() {
    return new Template(
        "verbatim_extensions",
        new StringReader(readTemplateFileToString()),
        new Configuration(Configuration.getVersion()));
  }

  /** Sets the template variable from a Download object. */
  private Map<String, Object> templateVariables(Download download) {
    String downloadTableName = DownloadUtils.downloadTableName(download.getKey());
    HashMap<String, Object> variables = new HashMap<>();
    if (download.getRequest().getFormat() == DownloadFormat.FASTA_ARCHIVE) {

    }
    Optional.ofNullable(DownloadRequestUtils.getVerbatimExtensions(download.getRequest()))
        .ifPresent(
            verbatimExtensions -> {
                List<ExtensionTable> extensionTables = new java.util.ArrayList<>(
                  verbatimExtensions.stream()
                    .map(ExtensionTable::new)
                    .toList());
                //FASTA Archives include the verbatim DNA data by default
                if (DownloadFormat.FASTA_ARCHIVE == download.getRequest().getFormat() &&
                   !verbatimExtensions.contains(Extension.DNA_DERIVED_DATA)) {
                  extensionTables.add(new ExtensionTable(Extension.DNA_DERIVED_DATA));
                }
                variables.put("verbatim_extensions", extensionTables);
            });
    variables.put("downloadTableName", downloadTableName);
    variables.put("interpretedTable", downloadTableName + "_interpreted");
    variables.put("tableName", download.getRequest().getType().toString().toLowerCase());
    return variables;
  }

  /** Generates the Hive query file querying extensions to the supplied writer. */
  @SneakyThrows
  public void generateExtensionsQueryHQL(Download download) {
    template().process(templateVariables(download), writer);
  }
}
