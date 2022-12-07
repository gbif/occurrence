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
package org.gbif.occurrence.download.file.dwca.archive;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.Archive;
import org.gbif.dwc.ArchiveField;
import org.gbif.dwc.ArchiveFile;
import org.gbif.dwc.MetaDescriptorWriter;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.download.hive.ExtensionTable;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Charsets;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.occurrence.download.file.dwca.archive.DwcDownloadsConstants.DESCRIPTOR_FILENAME;
import static org.gbif.occurrence.download.file.dwca.archive.DwcDownloadsConstants.EVENT_INTERPRETED_FILENAME;
import static org.gbif.occurrence.download.file.dwca.archive.DwcDownloadsConstants.METADATA_FILENAME;
import static org.gbif.occurrence.download.file.dwca.archive.DwcDownloadsConstants.MULTIMEDIA_FILENAME;
import static org.gbif.occurrence.download.file.dwca.archive.DwcDownloadsConstants.OCCURRENCE_INTERPRETED_FILENAME;
import static org.gbif.occurrence.download.file.dwca.archive.DwcDownloadsConstants.VERBATIM_FILENAME;

/**
 * Utility class for Darwin Core Archive handling during the download file creation.
 */
@Slf4j
@UtilityClass
public class DwcArchiveUtils {

  private static final String DEFAULT_DELIMITER = ";";

  /**
   * Creates a new archive file description for a DwC archive and sets the id field to the column of gbifID.
   * Used to generate the meta.xml with the help of the dwca-writer
   */
  public static ArchiveFile createArchiveFile(String filename, Term rowType, Iterable<? extends Term> columns) {
    return createArchiveFile(filename, rowType, columns, Collections.emptyMap());
  }

  /**
   * Adds archive fields derived from the columns.
   */
  private static void addArchiveFields(ArchiveFile af, Iterable<? extends Term> columns) {
    int index = 0;
    for (Term term : columns) {
      ArchiveField field = new ArchiveField();
      field.setIndex(index);
      field.setTerm(term);
      if (HiveColumnsUtils.isHiveArray(term)) {
        field.setDelimitedBy(DEFAULT_DELIMITER);
      }
      af.addField(field);
      index++;
    }
  }

  /**
   * Adds fields with default values.
   */
  private static void addDefaultValuedFields(ArchiveFile af,  Map<? extends Term,String> defaultColumns) {
    for (Map.Entry<? extends Term,String> defaultTerm : defaultColumns.entrySet()) {
      ArchiveField defaultField = new ArchiveField();
      defaultField.setTerm(defaultTerm.getKey());
      defaultField.setDefaultValue(defaultTerm.getValue());
      af.addField(defaultField);
    }
  }

  /**
   * Creates a new archive file description for a DwC archive and sets the id field to the column of gbifID.
   * Used to generate the meta.xml with the help of the dwca-writer
   */
  public static ArchiveFile createArchiveFile(String filename, Term rowType, Iterable<? extends Term> columns,
                                              Map<? extends Term,String> defaultColumns) {
    ArchiveFile af = buildBaseArchive(filename, rowType);

    //Add archive fields
    addArchiveFields(af, columns);
    addDefaultValuedFields(af, defaultColumns);

    ArchiveField coreId = af.getField(GbifTerm.gbifID);
    if (coreId == null) {
      throw new IllegalArgumentException("Archive columns MUST include the gbif:gbifID term");
    }
    af.setId(coreId);
    return af;
  }

  /**
   * Utility function that creates an archive with common/default settings.
   */
  private static ArchiveFile buildBaseArchive(String filename, Term rowType) {
    ArchiveFile af = new ArchiveFile();
    af.addLocation(filename);
    af.setRowType(rowType);
    af.setEncoding(Charsets.UTF_8.displayName());
    af.setIgnoreHeaderLines(1);
    af.setFieldsEnclosedBy(null);
    af.setFieldsTerminatedBy("\t");
    af.setLinesTerminatedBy("\n");
    return af;
  }

  /**
   * Creates a meta.xml occurrence descriptor file in the directory parameter.
   */
  public static void createOccurrenceArchiveDescriptor(File directory, Set<Extension> extensions) {
    createArchiveDescriptor(directory, OCCURRENCE_INTERPRETED_FILENAME, DwcTerm.Occurrence, extensions);
  }

  /**
   * Creates a meta.xml event descriptor file in the directory parameter.
   */
  public static void createEventArchiveDescriptor(File directory, Set<Extension> verbatimExtensions) {
    createArchiveDescriptor(directory, EVENT_INTERPRETED_FILENAME, DwcTerm.Event, verbatimExtensions);
  }

  public static void createArchiveDescriptor(File directory, String interpretedFileName, DwcTerm coreTerm, Set<Extension> verbatimExtensions) {
    log.info("Creating archive meta.xml descriptor");

    Archive downloadArchive = new Archive();

    ArchiveFile core = createArchiveFile(interpretedFileName, coreTerm, TermUtils.interpretedTerms(), TermUtils.identicalInterpretedTerms());
    downloadArchive.setCore(core);

    ArchiveFile verbatim = createArchiveFile(VERBATIM_FILENAME, coreTerm, TermUtils.verbatimTerms());
    downloadArchive.addExtension(verbatim);

    ArchiveFile multimedia = createArchiveFile(MULTIMEDIA_FILENAME, GbifTerm.Multimedia, TermUtils.multimediaTerms());
    downloadArchive.addExtension(multimedia);

    addVerbatimExtensionsFiles(verbatimExtensions, downloadArchive);

    addMetaFile(directory, downloadArchive);
  }

  /**
   * Creates an extension archive.
   */
  private ArchiveFile createExtensionArchiveFile(Extension extension) {
    ExtensionTable extensionTable = new ExtensionTable(extension);
    return createArchiveFile(extensionTable.getHiveTableName() + ".txt",
                             extensionTable.getTerm(),
                             extensionTable.getInterpretedFieldsAsTerms());
  }

  /**
   * Adds all the requested extensions.
   */
  private void addVerbatimExtensionsFiles(Set<Extension> verbatimExtensions, Archive downloadArchive) {
    if (verbatimExtensions != null) {
      verbatimExtensions.forEach(extension -> downloadArchive.addExtension(createExtensionArchiveFile(extension)));
    }
  }

  @SneakyThrows
  private void addMetaFile(File directory, Archive downloadArchive) {
    downloadArchive.setMetadataLocation(METADATA_FILENAME);
    File metaFile = new File(directory, DESCRIPTOR_FILENAME);
    MetaDescriptorWriter.writeMetaFile(metaFile, downloadArchive);
  }

}
