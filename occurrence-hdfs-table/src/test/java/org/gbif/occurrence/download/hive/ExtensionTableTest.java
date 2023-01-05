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

import org.gbif.api.vocabulary.Extension;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import static org.gbif.occurrence.download.hive.HiveColumns.cleanDelimitersInitializer;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for generated extensions tables.
 */
public class ExtensionTableTest {

  /**
   * All available extensions are correct.
   */
  @Test
  public void tableExtensionsTest() {
    ExtensionTable.tableExtensions()
      .forEach(extensionTable -> assertNotNull(extensionTable.getExtension()));
  }

  /**
   * Reserved word are treated correctly.
   */
  @Test
  public void reservedWordTest() {
    ExtensionTable extendedMofTable = new ExtensionTable(Extension.IDENTIFICATION);

    //Double underscore removed in the produced column name
    assertTrue(extendedMofTable.getFields().contains(cleanDelimitersInitializer("order_")));
  }

  @Test
  public void unsupportedExtensionTest() {
    assertThrows(IllegalArgumentException.class, () -> new ExtensionTable(Extension.SPECIES_PROFILE));
  }

  /**
   * Fields with special names.
   */
  @Test
  public void especialCasesTest() {
    ExtensionTable dnaDerivedTable = new ExtensionTable(Extension.DNA_DERIVED_DATA);

    //datasetkey and gbifid are processed without initializers, this is true for all tables
    assertTrue(dnaDerivedTable.getFields().contains(ExtensionTable.DATASET_KEY_FIELD));
    assertTrue(dnaDerivedTable.getFields().contains(ExtensionTable.GBIFID_FIELD));

    //Special cases are started with backticks to be compliant with Hive syntax
    assertTrue(dnaDerivedTable.getFields().contains(cleanDelimitersInitializer("`_16srecover`")));

    //Double underscore removed in the produced column name
    assertTrue(dnaDerivedTable.getFields().contains(cleanDelimitersInitializer("v__16srecover")));
  }

  /**
   * Audobon overloads/borrows terms from other extensions or namespaces.
   */
  @Test
  public void audobonBorrowedTermsTest() {
    //Audobon overloads some term names of Dc and DcTerms
    ExtensionTable audobonTable = new ExtensionTable(Extension.AUDUBON);
    Stream.of("rights", "creator", "source", "language", "type")
      .forEach(term -> {
        assertTrue(audobonTable.getFields().contains(cleanDelimitersInitializer("dc_" + term)));
        assertTrue(audobonTable.getFields().contains(cleanDelimitersInitializer("dcterms_" + term)));
      });
  }
}
