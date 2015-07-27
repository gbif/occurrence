package org.gbif.occurrence.processor.parsing;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Extension;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Maps;
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
    verbatim.setKey(fragment.getKey());
    verbatim.setDatasetKey(fragment.getDatasetKey());
    verbatim.setProtocol(fragment.getProtocol());
    verbatim.setLastCrawled(fragment.getHarvestedDate());

    // jackson untyped bind of data to object
    try {
      Map<String, Object> jsonMap = mapper.readValue(new ByteArrayInputStream(fragment.getData()), Map.class);

      // core terms
      for (Map.Entry<Term, String> entry : parseTermMap(jsonMap).entrySet()) {
        verbatim.setVerbatimField(entry.getKey(), entry.getValue());
      }

      // parse extensions
      if (jsonMap.containsKey("extensions")) {
        Map<Extension, List<Map<Term, String>>> extTerms = Maps.newHashMap();
        verbatim.setExtensions(extTerms);

        Map<String, Object> extensions = (Map<String, Object>) jsonMap.get("extensions");
        for (String rowType : extensions.keySet()) {
            // first pare into a term cause the extension lookup by rowType is very strict
            Term rowTypeTerm = termFactory.findTerm(rowType);
          Extension ext = Extension.fromRowType(rowTypeTerm.qualifiedName());
          if (ext == null) {
            LOG.debug("Ignore unknown extension {}", rowType);
          } else {
            List<Map<Term, String>> records = Lists.newArrayList();
            // transform records to term based map
            for (Map<String, Object> rawRecord : (List<Map<String, Object>>) extensions.get(rowType)) {
              records.add(parseTermMap(rawRecord));
            }
            extTerms.put(ext, records);
          }
        }
      }

    } catch (IOException e) {
      LOG.warn("Unable to parse JSON data, returning null VerbatimOccurrence", e);
    }

    return verbatim;
  }

  /**
   * Parses a simple string based map into a Term based map, ignoring any non term entries and not parsing nested
   * e.g. extensions data.
   */
  private static Map<Term, String> parseTermMap(Map<String, Object> data) {
    Map<Term, String> terms = Maps.newHashMap();

    for (Map.Entry<String, Object> entry : data.entrySet()) {
      String simpleTermName = entry.getKey();
      // ignore extensions key
      if (simpleTermName.equalsIgnoreCase("extensions")) {
        continue;
      }

      Object value = entry.getValue();
      if (value != null) {
        Term term = termFactory.findTerm(simpleTermName);
        terms.put(term, value.toString());
      }
    }
    return terms;
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
