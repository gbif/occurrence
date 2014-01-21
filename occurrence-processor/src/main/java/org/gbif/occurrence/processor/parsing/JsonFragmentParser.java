package org.gbif.occurrence.processor.parsing;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.parsing.xml.IdentifierExtractionResult;
import org.gbif.occurrence.persistence.api.Fragment;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Parses JSON-encoded fragments.
 */
public class JsonFragmentParser {

  private static final Logger LOG = LoggerFactory.getLogger(JsonFragmentParser.class);
  private static final TermFactory termFactory = TermFactory.instance();
  private static final ObjectMapper mapper = new ObjectMapper();

  // should not be instantiated
  private JsonFragmentParser() {
  }

  /**
   * Expects a fragment encoded in JSON (so, DwCA) and parses it into a VerbatimOccurrence.
   *
   * @param fragment of type JSON from a DwCA
   *
   * @return a VerbatimOccurrence parsed from the fragment's JSON
   */
  @Nullable
  public static VerbatimOccurrence parseRecord(Fragment fragment) {
    checkNotNull(fragment, "fragment can't be null");
    checkArgument(fragment.getFragmentType() == Fragment.FragmentType.JSON, "fragment must be of type JSON");

    VerbatimOccurrence verbatim = new VerbatimOccurrence();
    verbatim.setDatasetKey(fragment.getDatasetKey());
    verbatim.setProtocol(fragment.getProtocol());
    verbatim.setLastCrawled(fragment.getHarvestedDate());

    // jackson untyped bind of data to object
    try {
      Map<String, Object> jsonMap = mapper.readValue(new ByteArrayInputStream(fragment.getData()), Map.class);
      for (String simpleTermName : jsonMap.keySet()) {
        Term term = termFactory.findTerm(simpleTermName);
        if (term != null) {
          Object value = jsonMap.get(simpleTermName);
          verbatim.setField(term, value.toString());
        }
      }
    } catch (IOException e) {
      LOG.warn("Unable to parse JSON data, returning null VerbatimOccurrence", e);
    }

    return verbatim;
  }

  /**
   * Extracts the unique identifiers from this json snippet.
   *
   * @param datasetKey      the UUID of the dataset
   * @param jsonData        the raw byte array of the snippet's json
   * @param useOccurrenceId @return a set of
   */
  @Nullable
  public static IdentifierExtractionResult extractIdentifiers(UUID datasetKey, byte[] jsonData, boolean useTriplet,
    boolean useOccurrenceId) {
    checkNotNull(datasetKey, "datasetKey can't be null");
    checkNotNull(jsonData, "jsonData can't be null");

    IdentifierExtractionResult result = null;

    // jackson untyped bind of data to object
    ObjectMapper mapper = new ObjectMapper();
    try {
      Map<String, Object> jsonMap = mapper.readValue(new ByteArrayInputStream(jsonData), Map.class);
      Set<UniqueIdentifier> uniqueIds = Sets.newHashSet();

      // look for triplet
      if (useTriplet) {
        String ic = (String) jsonMap.get(DwcTerm.institutionCode.simpleName());
        String cc = (String) jsonMap.get(DwcTerm.collectionCode.simpleName());
        String cn = (String) jsonMap.get(DwcTerm.catalogNumber.simpleName());

        HolyTriplet holyTriplet = null;
        try {
          holyTriplet = new HolyTriplet(datasetKey, ic, cc, cn, null);
        } catch (IllegalArgumentException e) {
          // some of the triplet was null or empty, so it's not valid - that's highly suspicious, but could be ok...
          LOG.info("No holy triplet for json snippet in dataset [{}] and schema [{}], got error [{}]",
            new String[] {datasetKey.toString(), OccurrenceSchemaType.DWCA.toString(), e.getMessage()});
        }
        if (holyTriplet != null) {
          uniqueIds.add(holyTriplet);
        }
      }

      // look for occurrence id
      if (useOccurrenceId) {
        String occId = (String) jsonMap.get(DwcTerm.occurrenceID.simpleName());
        if (occId != null) {
          uniqueIds.add(new PublisherProvidedUniqueIdentifier(datasetKey, occId));
        }
      }

      if (!uniqueIds.isEmpty()) {
        result = new IdentifierExtractionResult(uniqueIds, null);
      }
    } catch (IOException e) {
      LOG.warn("Unable to parse JSON data, returning null IdentifierExtractionResult", e);
    }

    return result;
  }
}
