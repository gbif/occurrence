package org.gbif.occurrence.download.util;


/**
 * Common constants used to construct the Dwc download file.
 */
public class DwcDownloadsConstants {

  private DwcDownloadsConstants() {
    // default private constructor
  }

  public static final String METADATA_FILENAME = "metadata.xml";
  public static final String INTERPRETED_FILENAME = "occurrence.txt";
  public static final String VERBATIM_FILENAME = "verbatim.txt";
  public static final String MULTIMEDIA_FILENAME = "multimedia.txt";
  public static final String CITATIONS_FILENAME = "citations.txt";
  public static final String RIGHTS_FILENAME = "rights.txt";
  public static final String DESCRIPTOR_FILENAME = "meta.xml";
}
