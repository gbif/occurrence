package org.gbif.occurrence.download.util;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.dwc.text.Archive;
import org.gbif.dwc.text.ArchiveField;
import org.gbif.dwc.text.ArchiveFile;
import org.gbif.dwc.text.MetaDescriptorWriter;
import org.gbif.occurrence.common.TermUtils;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Charsets;
import freemarker.template.TemplateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.download.util.DwcDownloadsConstants.DESCRIPTOR_FILENAME;
import static org.gbif.occurrence.download.util.DwcDownloadsConstants.INTERPRETED_FILENAME;
import static org.gbif.occurrence.download.util.DwcDownloadsConstants.METADATA_FILENAME;
import static org.gbif.occurrence.download.util.DwcDownloadsConstants.MULTIMEDIA_FILENAME;
import static org.gbif.occurrence.download.util.DwcDownloadsConstants.VERBATIM_FILENAME;

/**
 * Utility class for Dwc archive handling during the download file creation.
 */
public class DwcArchiveUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DwcArchiveUtils.class);

  private DwcArchiveUtils() {
    // private empty constructor
  }


  /**
   * Creates a new archive file description for a dwc archive, but does not set any id field yet.
   * Used to generate the meta.xml with the help of the dwca-writer
   * 
   * @param index column index to start with
   */
  public static ArchiveFile createArchiveFile(String filename, Iterable<? extends Term> columns) {
    ArchiveFile af = buildBaseArchive(filename, DwcTerm.Occurrence.qualifiedName());
    int index = 0;
    for (Term term : columns) {
      ArchiveField field = new ArchiveField();
      field.setIndex(index);
      field.setTerm(term);
      af.addField(field);
      index++;
    }
    af.setId(af.getField(GbifTerm.gbifID));
    return af;
  }

  /**
   * Creates a new archive file description for a dwc archive, but does not set any id field yet.
   * Used to generate the meta.xml with the help of the dwca-writer
   * 
   * @param index column index to start with
   */
  public static ArchiveFile createMultimediaArchiveFile(String filename) {
    ArchiveFile archiveFile = buildBaseArchive(filename, Extension.MULTIMEDIA.getRowType());
    ArchiveField gbifIDField = new ArchiveField();
    gbifIDField.setIndex(0);
    archiveFile.setId(gbifIDField);
    final String[] multimediaFields = HeadersFileUtil.getMultimediaTableColumns();
    for (int i = 1; i < multimediaFields.length; i++) {
      ArchiveField field = new ArchiveField();
      field.setIndex(i);
      field.setTerm(TermFactory.instance().findTerm(multimediaFields[i]));
      archiveFile.addField(field);
    }

    return archiveFile;
  }

  /**
   * Utility function that creates an archive with common/default settings.
   */
  private static ArchiveFile buildBaseArchive(String filename, String rowType) {
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
   * Creates an meta.xml descriptor file in the directory parameter.
   */
  public static void createArchiveDescriptor(File directory) {
    LOG.info("Creating archive meta.xml descriptor");
    ArchiveFile occurrence = createArchiveFile(INTERPRETED_FILENAME, TermUtils.interpretedTerms());
    ArchiveFile verbatim = createArchiveFile(VERBATIM_FILENAME, TermUtils.verbatimTerms());
    ArchiveFile multimedia = createMultimediaArchiveFile(MULTIMEDIA_FILENAME);

    Archive downloadArchive = new Archive();
    downloadArchive.setCore(occurrence);
    downloadArchive.setMetadataLocation(METADATA_FILENAME);
    downloadArchive.addExtension(verbatim);
    downloadArchive.addExtension(multimedia);

    try {
      File metaFile = new File(directory, DESCRIPTOR_FILENAME);
      MetaDescriptorWriter.writeMetaFile(metaFile, downloadArchive);
    } catch (TemplateException e) {
      LOG.error("Error reading meta.xml template", e);
    } catch (IOException e) {
      LOG.error("Error creating meta.xml file", e);
    }
  }
}
