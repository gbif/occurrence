package org.gbif.occurrencestore.processor.parsing;

import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.parsing.xml.IdentifierExtractionResult;
import org.gbif.occurrencestore.common.model.HolyTriplet;
import org.gbif.occurrencestore.common.model.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrencestore.common.model.UniqueIdentifier;
import org.gbif.occurrencestore.persistence.api.Fragment;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrence;

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

    VerbatimOccurrence verbatim = null;

    // jackson untyped bind of data to object
    ObjectMapper mapper = new ObjectMapper();
    try {
      Map<String, Object> jsonMap = mapper.readValue(new ByteArrayInputStream(fragment.getData()), Map.class);

      String verbatimLatitude = (String) jsonMap.get(DwcTerm.verbatimLatitude.simpleName());
      String verbatimLongitude = (String) jsonMap.get(DwcTerm.verbatimLongitude.simpleName());
      String decimalLatitude = (String) jsonMap.get(DwcTerm.decimalLatitude.simpleName());
      String decimalLongitude = (String) jsonMap.get(DwcTerm.decimalLongitude.simpleName());

      String continent = (String) jsonMap.get(DwcTerm.continent.simpleName());
      String waterBody = (String) jsonMap.get(DwcTerm.waterBody.simpleName());

      String country = (String) jsonMap.get(DwcTerm.country.simpleName());
      String countryCode = (String) jsonMap.get(DwcTerm.countryCode.simpleName());

      verbatim = VerbatimOccurrence.builder().key(fragment.getKey()).datasetKey(fragment.getDatasetKey())
        .unitQualifier(fragment.getUnitQualifier())
        .institutionCode((String) jsonMap.get(DwcTerm.institutionCode.simpleName()))
        .collectionCode((String) jsonMap.get(DwcTerm.collectionCode.simpleName()))
        .catalogNumber((String) jsonMap.get(DwcTerm.catalogNumber.simpleName()))
        .scientificName((String) jsonMap.get(DwcTerm.scientificName.simpleName()))
        .author((String) jsonMap.get(DwcTerm.scientificNameAuthorship.simpleName()))
        .rank((String) jsonMap.get(DwcTerm.taxonRank.simpleName()))
        .kingdom((String) jsonMap.get(DwcTerm.kingdom.simpleName()))
        .phylum((String) jsonMap.get(DwcTerm.phylum.simpleName()))
        .klass((String) jsonMap.get(DwcTerm.class_.simpleName()))
        .order((String) jsonMap.get(DwcTerm.order.simpleName()))
        .family((String) jsonMap.get(DwcTerm.family.simpleName()))
        .genus((String) jsonMap.get(DwcTerm.genus.simpleName()))
        .species((String) jsonMap.get(DwcTerm.specificEpithet.simpleName()))
        .subspecies((String) jsonMap.get(DwcTerm.infraspecificEpithet.simpleName()))
          // prefer the decimal versions if both exist
        .latitude(decimalLatitude == null ? verbatimLatitude : decimalLatitude)
        .longitude(decimalLongitude == null ? verbatimLongitude : decimalLongitude)
        .latLongPrecision((String) jsonMap.get(DwcTerm.coordinatePrecision.simpleName()))
        .maxAltitude((String) jsonMap.get(DwcTerm.maximumElevationInMeters.simpleName()))
        .minAltitude((String) jsonMap.get(DwcTerm.minimumElevationInMeters.simpleName()))
          // no altitude precision
        .maxDepth((String) jsonMap.get(DwcTerm.maximumDepthInMeters.simpleName()))
        .minDepth((String) jsonMap.get(DwcTerm.minimumDepthInMeters.simpleName()))
          // no depth precision
        .continentOrOcean(continent == null ? waterBody : continent)
        .country(countryCode == null ? country : countryCode)
        .stateOrProvince((String) jsonMap.get(DwcTerm.stateProvince.simpleName()))
        .county((String) jsonMap.get(DwcTerm.county.simpleName()))
        .collectorName((String) jsonMap.get(DwcTerm.recordedBy.simpleName()))
        .locality((String) jsonMap.get(DwcTerm.locality.simpleName()))
        .year((String) jsonMap.get(DwcTerm.year.simpleName())).month((String) jsonMap.get(DwcTerm.month.simpleName()))
        .day((String) jsonMap.get(DwcTerm.day.simpleName()))
        .occurrenceDate((String) jsonMap.get(DwcTerm.eventDate.simpleName()))
        .basisOfRecord((String) jsonMap.get(DwcTerm.basisOfRecord.simpleName()))
        .identifierName((String) jsonMap.get(DwcTerm.identifiedBy.simpleName()))
          // no day-, month- or year-identified in DwcTerm
        .dateIdentified((String) jsonMap.get(DwcTerm.dateIdentified.simpleName())).modified(System.currentTimeMillis())
        .build();
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
