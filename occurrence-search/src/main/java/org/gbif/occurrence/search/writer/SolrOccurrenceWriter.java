/**
 *
 */
package org.gbif.occurrence.search.writer;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.dwc.terms.DwcTerm;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import static org.gbif.common.search.util.QueryUtils.toDateQueryFormat;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.BASIS_OF_RECORD;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.CATALOG_NUMBER;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COLLECTION_CODE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.CONTINENT;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COORDINATE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COUNTRY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.DATASET_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.DEPTH;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.ELEVATION;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.EVENT_DATE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.HAS_COORDINATE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.INSTITUTION_CODE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.ISSUE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LAST_INTERPRETED;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LATITUDE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LONGITUDE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.MEDIA_TYPE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.MONTH;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.PUBLISHING_COUNTRY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.RECORDED_BY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.RECORD_NUMBER;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.SPATIAL_ISSUES;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.TAXON_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.TYPE_STATUS;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.YEAR;


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

  // SolrServer that stores the occurrence records
  private final SolrServer solrServer;

  private final int commitWithinMs;

  /**
   * Default constructor.
   */
  public SolrOccurrenceWriter(SolrServer solrServer, int commitWithinMs) {
    this.solrServer = solrServer;
    this.commitWithinMs = commitWithinMs;
  }

  /**
   * Default constructor.
   */
  public SolrOccurrenceWriter(SolrServer solrServer) {
    this.solrServer = solrServer;
    this.commitWithinMs = -1;
  }


  /**
   * Validate if the latitude and longitude values are not null and have correct values: latitude:[-90.0,90.0] and
   * longitude[-180.0,180.0].
   */
  private static boolean isValidCoordinate(Double latitude, Double longitude) {
    if (latitude != null && longitude != null) {
      return LAT_RANGE.contains(latitude) && LNG_RANGE.contains(longitude);
    }
    return false;
  }


  public void delete(Occurrence input) throws IOException, SolrServerException {
    solrServer.deleteById(input.getKey().toString(), commitWithinMs);
  }

  /**
   * Processes the occurrence object.
   */
  public void update(Occurrence input) throws IOException, SolrServerException {
    solrServer.add(buildOccSolrDocument(input), commitWithinMs);
  }

  /**
   * Populates the Solr document using the occurrence object.
   */
  private SolrInputDocument buildOccSolrDocument(Occurrence occurrence) {
    SolrInputDocument doc = new SolrInputDocument();
    final Double latitude = occurrence.getDecimalLatitude();
    final Double longitude = occurrence.getDecimalLongitude();

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
      occurrence.getPublishingCountry() == null ? null : occurrence.getPublishingCountry().getIso2LetterCode());
    doc.setField(CONTINENT.getFieldName(), occurrence.getContinent() == null ? null : occurrence.getContinent().name());
    doc.setField(DATASET_KEY.getFieldName(), occurrence.getDatasetKey().toString());
    Set<Integer> taxonKey = buildTaxonKey(occurrence);
    if (!taxonKey.isEmpty()) {
      doc.setField(TAXON_KEY.getFieldName(), taxonKey);
    } else {
      doc.setField(TAXON_KEY.getFieldName(), null);
    }
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
    if (isValidCoordinate(latitude, longitude)) {
      doc.setField(COORDINATE.getFieldName(), COORD_JOINER.join(latitude, longitude));
    } else {
      doc.setField(COORDINATE.getFieldName(), null);
    }
    doc.setField(MEDIA_TYPE.getFieldName(), buildMediaType(occurrence));
    doc.setField(ISSUE.getFieldName(), occurrence.getIssues());
    return doc;
  }


  /**
   * Returns a nullable set of String that contains the media types present in the occurrence object.
   */
  private Set<String> buildMediaType(Occurrence occurrence) {
    Set<String> mediaTypes = null;
    if (occurrence.getMedia() != null && !occurrence.getMedia().isEmpty()) {
      mediaTypes = Sets.newHashSet();
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
  private Set<Integer> buildTaxonKey(Occurrence occurrence) {

    Set<Integer> taxonKey = new HashSet<Integer>();

    if (occurrence.getTaxonKey() != null) {
      taxonKey.add(occurrence.getTaxonKey());
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

}
