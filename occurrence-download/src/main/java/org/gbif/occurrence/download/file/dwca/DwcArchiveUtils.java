package org.gbif.occurrence.download.file.dwca;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwca.io.Archive;
import org.gbif.dwca.io.ArchiveField;
import org.gbif.dwca.io.ArchiveFile;
import org.gbif.dwca.io.MetaDescriptorWriter;
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.DESCRIPTOR_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.INTERPRETED_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.METADATA_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.MULTIMEDIA_FILENAME;
import static org.gbif.occurrence.download.file.dwca.DwcDownloadsConstants.VERBATIM_FILENAME;

/**
 * Utility class for Dwc archive handling during the download file creation.
 */
public class DwcArchiveUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DwcArchiveUtils.class);
  private static final String DEFAULT_DELIMITER = ";";

  /**
   * Creates a new archive file description for a dwc archive and sets the id field to the column of gbifID.
   * Used to generate the meta.xml with the help of the dwca-writer
   */
  public static ArchiveFile createArchiveFile(String filename, Term rowType, Iterable<? extends Term> columns) {
    ArchiveFile af = buildBaseArchive(filename, rowType);
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
   * Creates an meta.xml descriptor file in the directory parameter.
   */
  public static void createArchiveDescriptor(File directory) {
    LOG.info("Creating archive meta.xml descriptor");

    Archive downloadArchive = new Archive();
    downloadArchive.setMetadataLocation(METADATA_FILENAME);

    ArchiveFile occurrence = createArchiveFile(INTERPRETED_FILENAME, DwcTerm.Occurrence, TermUtils.interpretedTerms());
    downloadArchive.setCore(occurrence);

    ArchiveFile verbatim = createArchiveFile(VERBATIM_FILENAME, DwcTerm.Occurrence, TermUtils.verbatimTerms());
    downloadArchive.addExtension(verbatim);

    ArchiveFile multimedia = createArchiveFile(MULTIMEDIA_FILENAME, GbifTerm.Multimedia, TermUtils.multimediaTerms());
    downloadArchive.addExtension(multimedia);

    try {
      File metaFile = new File(directory, DESCRIPTOR_FILENAME);
      MetaDescriptorWriter.writeMetaFile(metaFile, downloadArchive);
    } catch (IOException e) {
      LOG.error("Error creating meta.xml file", e);
    }
  }

  /**
   * Hidden constructor.
   */
  private DwcArchiveUtils() {
    // private empty constructor
  }
}
