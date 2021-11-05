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
package org.gbif.occurrence.download.file.specieslist;


import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SpeciesListCollectorTest {


  /**
   * test groupby results
   * 
   * @throws IOException
   */
  @Test
  public void testReadAndWrite() throws IOException {
    List<Map<String, String>> occurrenceRecords = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      Map<String, String> speciesRecord = new HashMap<>();
      speciesRecord.put(GbifTerm.taxonKey.simpleName(), Long.toString(i%3));
      speciesRecord.put(DwcTerm.scientificName.simpleName(), "xxxx");
      speciesRecord.put(DwcTerm.taxonRank.simpleName(), "ANIMALIA");
      occurrenceRecords.add(speciesRecord);
    }

    SpeciesListCollector collector = new SpeciesListCollector();
    occurrenceRecords.forEach(collector::collect);
    assertEquals(3, collector.getDistinctSpecies().size());
  }

  /**
   * Tests the SpeciesCollector for taxonKeys with existing and non-existing values for numOfOccurrences.
   */
  @Test
  public void testAggregations() {
    //Even if the numOfOccurrence is null, it should count for 1
    Map<String,String> speciesRecord1 = buildTestRecord("1", null);
    Map<String,String> speciesRecord2 = buildTestRecord("2", 100L);
    Map<String,String> speciesRecord3 = buildTestRecord("1", 33L);
    Map<String,String> speciesRecord4 = buildTestRecord("1", 100L);
    SpeciesListCollector collector = new SpeciesListCollector();
    collector.collect(speciesRecord1);
    collector.collect(speciesRecord2);
    collector.collect(speciesRecord3);
    collector.collect(speciesRecord4);
    assertEquals(collector.getByTaxonKey("1").get(GbifTerm.numberOfOccurrences.simpleName()), "134");
    assertEquals(collector.getByTaxonKey("2").get(GbifTerm.numberOfOccurrences.simpleName()), "100");
  }

  /**
   * Creates test records using taxonKey and numOfOccurrences values.
   */
  private Map<String,String> buildTestRecord(String taxonKey, Long numOfOccurrences) {
    Map<String, String> speciesRecord = new HashMap<>();
    if (Objects.nonNull(numOfOccurrences)) {
      speciesRecord.put(GbifTerm.numberOfOccurrences.simpleName(), Long.toString(numOfOccurrences));
    }
    speciesRecord.put(GbifTerm.taxonKey.simpleName(), taxonKey);
    return  speciesRecord;
  }
}
