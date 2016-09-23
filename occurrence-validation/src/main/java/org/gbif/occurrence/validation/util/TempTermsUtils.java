package org.gbif.occurrence.validation.util;

import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

/**
 *
 */
public class TempTermsUtils {

  public static Term[] buildTermMapping(String[] columns) {

    TermFactory TERM_FACTORY = TermFactory.instance();

    Term[] columnMapping = new Term[columns.length];
    for (int i = 0; i < columns.length; i++) {
      columnMapping[i] = TERM_FACTORY.findTerm(columns[i]);
    }
    return columnMapping;
  }
}
