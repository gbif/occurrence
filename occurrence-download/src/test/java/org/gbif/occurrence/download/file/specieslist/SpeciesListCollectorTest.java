package org.gbif.occurrence.download.file.specieslist;


import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.junit.Test;

public class SpeciesListCollectorTest {


  /**
   * test groupby results
   * 
   * @throws IOException
   */
  @Test
  public void testReadAndWrite() throws IOException {
    List<Map<String, String>> filteredResult = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      Map<String, String> speciesRecord = new HashMap<String, String>();
      speciesRecord.put(GbifTerm.taxonKey.simpleName(), Long.toString(i % 3));
      speciesRecord.put(DwcTerm.scientificName.simpleName(), "xxxx");
      speciesRecord.put(GbifTerm.numOfOccurrences.simpleName(), Long.toString(i * 2));
      speciesRecord.put(DwcTerm.taxonRank.simpleName(), "ANIMALIA");
      filteredResult.add(speciesRecord);
    }

    SpeciesListCollector collector = new SpeciesListCollector();
    filteredResult.iterator().forEachRemaining(record -> collector.computeDistinctSpecies(record));

    assertEquals(3, collector.getDistinctSpecies().size());
  }
}
