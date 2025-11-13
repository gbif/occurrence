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

import org.apache.avro.Schema;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.gbif.dwc.terms.TermFactory;
import org.junit.jupiter.api.Test;

import static org.gbif.occurrence.download.hive.HiveColumns.cleanDelimitersInitializer;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for generated extensions tables.
 */
public class ExtensionTableTest {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  /**
   * All available extensions are correct.
   */
  @Test
  public void tableExtensionsTest() {
    ExtensionTable.tableExtensions()
      .forEach(extensionTable -> assertNotNull(extensionTable.getExtension()));
  }

  /**
   * Reserved words are treated correctly.
   */
  @Test
  public void reservedWordTest() {
    ExtensionTable extendedMofTable = new ExtensionTable(Extension.IDENTIFICATION);

    //Double underscore removed in the produced column name
    assertTrue(extendedMofTable.getFieldInitializers().contains(cleanDelimitersInitializer("order")));
  }

  @Test
  public void unsupportedExtensionTest() {
    assertThrows(IllegalArgumentException.class, () -> new ExtensionTable(Extension.SPECIES_PROFILE));
  }

  /**
   * Fields with special names.
   */
  @Test
  public void specialCasesTest() {
    ExtensionTable dnaDerivedTable = new ExtensionTable(Extension.DNA_DERIVED_DATA);

    //datasetkey and gbifid are processed without initializers, this is true for all tables
    assertTrue(dnaDerivedTable.getFieldInitializers().contains(ExtensionTable.DATASET_KEY_FIELD));
    assertTrue(dnaDerivedTable.getFieldInitializers().contains(ExtensionTable.GBIFID_FIELD));

    //Special cases are started with backticks to be compliant with Hive syntax
    assertTrue(dnaDerivedTable.getFieldInitializers().contains(cleanDelimitersInitializer("_16srecover")));
    assertEquals("cleanDelimiters(`_16srecover`) AS `16srecover`", cleanDelimitersInitializer("_16srecover"));

    // Double underscore removed in the produced column name
    assertTrue(dnaDerivedTable.getFieldInitializers().contains(cleanDelimitersInitializer("v_16srecover")));
    assertEquals("cleanDelimiters(v_16srecover) AS v_16srecover", cleanDelimitersInitializer("v_16srecover"));
  }

  /**
   * Audubon overloads/borrows terms from other extensions or namespaces.
   */
  @Test
  public void audobonBorrowedTermsTest() {
    //Audubon overloads some term names of Dc and DcTerms
    ExtensionTable audobonTable = new ExtensionTable(Extension.AUDUBON);
    Stream.of("rights", "creator", "source", "language", "type")
      .forEach(term -> {
        assertTrue(audobonTable.getFieldInitializers().contains(cleanDelimitersInitializer("dc_" + term)));
        assertTrue(audobonTable.getFieldInitializers().contains(cleanDelimitersInitializer("dcterms_" + term)));
      });
  }

  /**
   * Check all terms are known.
   */
  @Test
  public void interpretedFieldsAsTermsTest() {
    for (Extension ext : Extension.availableExtensions()) {
      ExtensionTable extensionTable = new ExtensionTable(ext);

      for (Term t : extensionTable.getInterpretedFieldsAsTerms()) {
        if (t instanceof UnknownError) {
          fail("Unknown term "+t);
        }
      }
    }
  }

  /**
   * Check the getInterpretedFields, getInterpretedFieldsAsTerms and getVerbatimFields methods use the same order.
   */
  @Test
  public void consistentFieldTermsOrderTest() {
    for (Extension ext : Extension.availableExtensions()) {
      ExtensionTable extensionTable = new ExtensionTable(ext);

      Set<String> fields = extensionTable.getInterpretedFields();
      List<Term> terms = extensionTable.getInterpretedFieldsAsTerms();
      Set<String> verbatims = extensionTable.getVerbatimFields();
      assertEquals(fields.size(), terms.size());
      assertEquals(fields.size(), verbatims.size());

      int i = 0;
      Iterator<String> verbatimsIterator = verbatims.iterator();
      for (String fieldString : fields) {
        Term term = terms.get(i++);
        String verbatimString = verbatimsIterator.next();

        if (term.equals(GbifTerm.gbifID)) {
          assertEquals(ExtensionTable.GBIFID_FIELD, fieldString);
          assertEquals(ExtensionTable.GBIFID_FIELD, verbatimString);
        } else if (term.equals(GbifTerm.datasetKey)) {
          assertEquals(ExtensionTable.DATASET_KEY_FIELD, fieldString);
          assertEquals(ExtensionTable.DATASET_KEY_FIELD, verbatimString);
        } else {
          String unescapedFieldName = fieldString.replaceAll("`", "").replaceAll("^([0-9])", "_\\1");
          Schema.Field field = extensionTable.getSchema().getField(unescapedFieldName);
          assertEquals(TERM_FACTORY.findPropertyTerm(field.doc()), term);

          assertEquals(unescapedFieldName.replaceAll("^_", ""), verbatimString.replace("v_", ""));
        }
      }
    }
  }
}
