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
package org.gbif.occurrence.download.file.dwca;

import org.gbif.api.vocabulary.Extension;
import org.gbif.occurrence.download.hive.ExtensionTable;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.supercsv.cellprocessor.ift.CellProcessor;

public class CsvExtension {

  private final String[] columns;

  private final CellProcessor[] processors;

  public CsvExtension(String rowType) {
    this(Extension.fromRowType(rowType));
  }

  public CsvExtension(Extension extension) {
    ExtensionTable table = new ExtensionTable(extension);
    Set<String> interpretedFields = table.getInterpretedFields();
    columns = interpretedFields.toArray(new String[0]);
    processors = interpretedFields.stream().map( i-> new DownloadDwcaActor.CleanStringProcessor()).toArray(CellProcessor[]::new);
  }

  public String[] getColumns() {
    return columns;
  }

  public CellProcessor[] getProcessors() {
    return processors;
  }

  public static class CsvExtensionFactory {

    private static final Map<Extension,CsvExtension> CSV_EXTENSION_MAP = ExtensionTable.getSupportedExtensions()
                                                                          .stream()
                                                                          .collect(Collectors.toMap(Function.identity(), CsvExtension::new));

    public static CsvExtension getCsvExtension(String rowType) {
      return  CSV_EXTENSION_MAP.get(Extension.fromRowType(rowType));
    }
  }

}
