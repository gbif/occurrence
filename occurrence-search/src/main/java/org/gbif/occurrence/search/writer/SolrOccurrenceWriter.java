package org.gbif.occurrence.search.writer;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.gbif.occurrence.search.solr.OccurrenceSolrField;

import static org.gbif.common.search.solr.QueryUtils.toDateQueryFormat;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.BASIS_OF_RECORD;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.CATALOG_NUMBER;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.CLASS_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COLLECTION_CODE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.CONTINENT;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COORDINATE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COUNTRY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.DATASET_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.DEPTH;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.ELEVATION;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.ESTABLISHMENT_MEANS;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.EVENT_DATE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.FAMILY_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.FULL_TEXT;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.GENUS_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.HAS_COORDINATE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.INSTITUTION_CODE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.ISSUE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.KINGDOM_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LAST_INTERPRETED;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LATITUDE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LONGITUDE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.MEDIA_TYPE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.MONTH;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.OCCURRENCE_ID;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.ORDER_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.PHYLUM_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.PUBLISHING_COUNTRY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.RECORDED_BY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.RECORD_NUMBER;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.SPATIAL_ISSUES;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.SPECIES_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.SUBGENUS_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.TAXON_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.TYPE_STATUS;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.YEAR;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.REPATRIATED;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.ORGANISM_ID;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.STATE_PROVINCE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.WATER_BODY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LOCALITY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.PROTOCOL;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LICENSE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.CRAWL_ID;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.PUBLISHING_ORGANIZATION_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.NETWORK_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.EVENT_ID;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.PARENT_EVENT_ID;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.SAMPLING_PROTOCOL;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.INSTALLATION_KEY;



/**
 * Utility class that stores an Occurrence record into a Solr index.
 */
public class SolrOccurrenceWriter {

  // Joins coordinates with ','
  private static final Joiner COORD_JOINER = Joiner.on(',').useForNull("");

  // Allowed latitude range
  private static final Range<Double> LAT_RANGE = Range.closed(-90.0, 90.0);

  // Allowed longitude range
  private static final Range<Double> LNG_RANGE = Range.closed(-180.0, 180.0);

  // SolrClient that stores the occurrence records
  private final SolrClient solrClient;

  private final int commitWithinMs;


  /**
   * Default constructor.
   */
  public SolrOccurrenceWriter(SolrClient solrClient, int commitWithinMs) {
    this.solrClient = solrClient;
    this.commitWithinMs = commitWithinMs;
  }

  /**
   * Default constructor.
   */
  public SolrOccurrenceWriter(SolrClient solrClient) {
    this.solrClient = solrClient;
    commitWithinMs = -1;
  }


  /**
   * Validate if the latitude and longitude values are not null and have correct values: latitude:[-90.0,90.0] and
   * longitude[-180.0,180.0].
   */
  private static boolean isValidCoordinate(Double latitude, Double longitude) {
    return latitude != null && longitude != null && LAT_RANGE.contains(latitude) && LNG_RANGE.contains(longitude);
  }


  public void delete(Occurrence input) throws IOException, SolrServerException {
    solrClient.deleteById(input.getKey().toString(), commitWithinMs);
  }

  public void delete(List<Occurrence> input) throws IOException, SolrServerException {
     solrClient.deleteById(input.stream().map(occurrence -> occurrence.getKey().toString()).collect(Collectors.toList()),
                           commitWithinMs);
  }

  /**
   * Processes the occurrence object.
   */
  public void update(Occurrence input) throws IOException, SolrServerException {
    solrClient.add(buildOccSolrDocument(input));
  }

  /**
    * Processes a batch of occurrence objects.
   */
  public void update(List<Occurrence> input) throws IOException, SolrServerException {
    solrClient.add(input.stream().map(SolrOccurrenceWriter::buildOccSolrDocument).collect(Collectors.toList()));
  }

  /**
   * Populates the Solr document using the occurrence object.
   */
  public static SolrInputDocument buildOccSolrDocument(Occurrence occurrence) {
    SolrInputDocument doc = new SolrInputDocument();
    Double latitude = occurrence.getDecimalLatitude();
    Double longitude = occurrence.getDecimalLongitude();

    doc.setField(KEY.getFieldName(), occurrence.getKey());
    doc.setField(YEAR.getFieldName(), occurrence.getYear());
    doc.setField(MONTH.getFieldName(), occurrence.getMonth());
    doc.setField(BASIS_OF_RECORD.getFieldName(),
                 occurrence.getBasisOfRecord() == null ? null : occurrence.getBasisOfRecord().name());
    doc.setField(CATALOG_NUMBER.getFieldName(), occurrence.getVerbatimField(DwcTerm.catalogNumber));
    doc.setField(RECORDED_BY.getFieldName(), occurrence.getVerbatimField(DwcTerm.recordedBy));
    doc.setField(TYPE_STATUS.getFieldName(),
                 occurrence.getTypeStatus() == null ? null : occurrence.getTypeStatus().name());
    doc.setField(RECORD_NUMBER.getFieldName(), occurrence.getVerbatimField(DwcTerm.recordNumber));
    doc.setField(COUNTRY.getFieldName(),
                 occurrence.getCountry() == null ? null : occurrence.getCountry().getIso2LetterCode());
    doc.setField(PUBLISHING_COUNTRY.getFieldName(),
                 occurrence.getPublishingCountry() == null
                   ? null
                   : occurrence.getPublishingCountry().getIso2LetterCode());
    doc.setField(CONTINENT.getFieldName(), occurrence.getContinent() == null ? null : occurrence.getContinent().name());
    doc.setField(DATASET_KEY.getFieldName(), occurrence.getDatasetKey().toString());
    Set<Integer> taxonKey = buildTaxonKey(occurrence);
    doc.setField(TAXON_KEY.getFieldName(), taxonKey.isEmpty()? null : taxonKey);

    doc.setField(KINGDOM_KEY.getFieldName(), occurrence.getKingdomKey());
    doc.setField(PHYLUM_KEY.getFieldName(), occurrence.getPhylumKey());
    doc.setField(CLASS_KEY.getFieldName(), occurrence.getClassKey());
    doc.setField(ORDER_KEY.getFieldName(), occurrence.getOrderKey());
    doc.setField(FAMILY_KEY.getFieldName(), occurrence.getFamilyKey());
    doc.setField(GENUS_KEY.getFieldName(), occurrence.getGenusKey());
    doc.setField(SUBGENUS_KEY.getFieldName(), occurrence.getSubgenusKey());
    doc.setField(SPECIES_KEY.getFieldName(), occurrence.getSpeciesKey());
    doc.setField(ELEVATION.getFieldName(), occurrence.getElevation());
    doc.setField(DEPTH.getFieldName(), occurrence.getDepth());
    doc.setField(INSTITUTION_CODE.getFieldName(), occurrence.getVerbatimField(DwcTerm.institutionCode));
    doc.setField(COLLECTION_CODE.getFieldName(), occurrence.getVerbatimField(DwcTerm.collectionCode));
    doc.setField(SPATIAL_ISSUES.getFieldName(), occurrence.hasSpatialIssue());
    doc.setField(LATITUDE.getFieldName(), latitude);
    doc.setField(LONGITUDE.getFieldName(), longitude);
    doc.setField(HAS_COORDINATE.getFieldName(), latitude != null && longitude != null);
    doc.setField(EVENT_DATE.getFieldName(),
                 occurrence.getEventDate() != null ? toDateQueryFormat(occurrence.getEventDate()) : null);
    doc.setField(LAST_INTERPRETED.getFieldName(),
                 occurrence.getLastInterpreted() != null ? toDateQueryFormat(occurrence.getLastInterpreted()) : null);

    doc.setField(COORDINATE.getFieldName(), isValidCoordinate(latitude, longitude)? COORD_JOINER.join(latitude, longitude) : null);
    doc.setField(MEDIA_TYPE.getFieldName(), buildMediaType(occurrence));
    doc.setField(ISSUE.getFieldName(), buildIssue(occurrence.getIssues()));
    doc.setField(ESTABLISHMENT_MEANS.getFieldName(),
                 occurrence.getEstablishmentMeans() == null ? null : occurrence.getEstablishmentMeans().name());
    doc.setField(OCCURRENCE_ID.getFieldName(), occurrence.getVerbatimField(DwcTerm.occurrenceID));
    doc.setField(FULL_TEXT.getFieldName(), FullTextFieldBuilder.buildFullTextField(occurrence));
    doc.setField(REPATRIATED.getFieldName(), isRepatriated(occurrence).orElse(null));
    doc.setField(ORGANISM_ID.getFieldName(), occurrence.getVerbatimField(DwcTerm.organismID));
    doc.setField(STATE_PROVINCE.getFieldName(), occurrence.getStateProvince());
    doc.setField(WATER_BODY.getFieldName(), occurrence.getWaterBody());
    doc.setField(LOCALITY.getFieldName(), occurrence.getVerbatimField(DwcTerm.locality));
    doc.setField(PROTOCOL.getFieldName(), occurrence.getProtocol() == null ? null : occurrence.getProtocol().name());
    doc.setField(CRAWL_ID.getFieldName(), occurrence.getCrawlId());
    doc.setField(PUBLISHING_ORGANIZATION_KEY.getFieldName(),
                 occurrence.getPublishingOrgKey() == null ? null : occurrence.getPublishingOrgKey().toString());
    doc.setField(LICENSE.getFieldName(), occurrence.getLicense() == null ? null : occurrence.getLicense().name());
    doc.setField(NETWORK_KEY.getFieldName(),
      occurrence.getNetworkKeys() == null || occurrence.getNetworkKeys().isEmpty()? null : occurrence.getNetworkKeys().stream().map(UUID::toString).collect(Collectors.toList()));
    doc.setField(INSTALLATION_KEY.getFieldName(),
      occurrence.getInstallationKey() == null ? null : occurrence.getInstallationKey().toString());
    doc.setField(EVENT_ID.getFieldName(),occurrence.getVerbatimField(DwcTerm.eventID));
    doc.setField(PARENT_EVENT_ID.getFieldName(),occurrence.getVerbatimField(DwcTerm.parentEventID));
    doc.setField(SAMPLING_PROTOCOL.getFieldName(),occurrence.getVerbatimField(DwcTerm.samplingProtocol));
    return doc;
  }

  /**
   * Returns a nullable set of String that contains the result of .name() of each issues.
   */
  private static Set<String> buildIssue(Set<OccurrenceIssue> occurrenceIssues) {
    Set<String> issuesList = null;
    if (occurrenceIssues != null && !occurrenceIssues.isEmpty()) {
      issuesList = Sets.newHashSetWithExpectedSize(occurrenceIssues.size());
      for (OccurrenceIssue issue : occurrenceIssues) {
        issuesList.add(issue.name().toUpperCase());
      }
    }
    return issuesList;
  }

  /**
   * Returns a nullable set of String that contains the media types present in the occurrence object.
   */
  private static Set<String> buildMediaType(Occurrence occurrence) {
    Set<String> mediaTypes = null;
    if (occurrence.getMedia() != null && !occurrence.getMedia().isEmpty()) {
      mediaTypes = Sets.newHashSetWithExpectedSize(occurrence.getMedia().size());
      for (MediaObject mediaObject : occurrence.getMedia()) {
        if (mediaObject.getType() != null) {
          mediaTypes.add(mediaObject.getType().name().toUpperCase());
        }
      }
    }
    return mediaTypes;
  }

  /**
   * Return a set of integer that contains the taxon key values.
   */
  private static Set<Integer> buildTaxonKey(Occurrence occurrence) {

    Set<Integer> taxonKey = new HashSet<>();

    if (occurrence.getTaxonKey() != null) {
      taxonKey.add(occurrence.getTaxonKey());
    }

    if (occurrence.getAcceptedTaxonKey() != null) {
      taxonKey.add(occurrence.getAcceptedTaxonKey());
    }

    if (occurrence.getKingdomKey() != null) {
      taxonKey.add(occurrence.getKingdomKey());
    }

    if (occurrence.getPhylumKey() != null) {
      taxonKey.add(occurrence.getPhylumKey());
    }

    if (occurrence.getClassKey() != null) {
      taxonKey.add(occurrence.getClassKey());
    }

    if (occurrence.getOrderKey() != null) {
      taxonKey.add(occurrence.getOrderKey());
    }

    if (occurrence.getFamilyKey() != null) {
      taxonKey.add(occurrence.getFamilyKey());
    }

    if (occurrence.getGenusKey() != null) {
      taxonKey.add(occurrence.getGenusKey());
    }
    if (occurrence.getSubgenusKey() != null) {
      taxonKey.add(occurrence.getSubgenusKey());
    }

    if (occurrence.getSpeciesKey() != null) {
      taxonKey.add(occurrence.getSpeciesKey());
    }

    return taxonKey;
  }

  /**
   * Determines if the occurrence record has been repatriated.
   */
  private static Optional<Boolean> isRepatriated(Occurrence occurrence) {
    if (occurrence.getPublishingCountry() != null && occurrence.getCountry() !=  null) {
      return  Optional.of(!occurrence.getPublishingCountry().equals(occurrence.getCountry()));
    }
    return Optional.empty();
  }

}
