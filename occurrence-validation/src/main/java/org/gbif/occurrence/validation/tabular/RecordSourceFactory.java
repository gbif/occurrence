package org.gbif.occurrence.validation.tabular;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.api.RecordSource;
import org.gbif.tabular.MappedTabularFiles;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 *
 */
public class RecordSourceFactory {

  public static RecordSource fromDelimited(File sourceFile, char delimiterChar, boolean headerIncluded, Term[] columnMapping)
          throws FileNotFoundException {
    return new TabularFileReader(MappedTabularFiles.newTermMappedTabularFileReader(
          new FileInputStream(sourceFile), delimiterChar, headerIncluded, columnMapping));
  }

}
