package org.gbif.occurrence.download.hive;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.junit.Test;

import static org.junit.Assert.fail;

public class ExtensionTableTest {

  /**
   * Check all terms are known.
   */
  @Test
  public void interpretedFieldsAsTermsTest() {
    for (Extension ext : Extension.availableExtensions()) {
      System.out.println("Extension " + ext);
      ExtensionTable extensionTable = new ExtensionTable(ext);

      for (Term t : extensionTable.getInterpretedFieldsAsTerms()) {
        //System.out.println(t);
        if (t instanceof UnknownError) {
          fail("Unknown term "+t);
        }
      }
    }
  }
}
