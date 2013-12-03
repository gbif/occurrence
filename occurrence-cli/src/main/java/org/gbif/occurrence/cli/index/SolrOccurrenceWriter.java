/**
 * 
 */
package org.gbif.occurrence.cli.index;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.occurrencestore.util.BasisOfRecordConverter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Range;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import static org.gbif.common.search.util.QueryUtils.toDateQueryFormat;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.ALTITUDE;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.BASIS_OF_RECORD;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.CATALOG_NUMBER;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.COLLECTION_CODE;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.COLLECTOR_NAME;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.COORDINATE;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.COUNTRY;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.DATASET_KEY;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.DATE;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.DEPTH;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.GEOREFERENCED;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.GEOSPATIAL_ISSUE;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.INSTITUTION_CODE;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.KEY;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.LATITUDE;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.LONGITUDE;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.MODIFIED;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.MONTH;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.PUBLISHING_COUNTRY;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.TAXON_KEY;
import static org.gbif.occurrencestore.search.solr.OccurrenceSolrField.YEAR;


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

  // Basis of record converter
  private static final BasisOfRecordConverter BOR_CONVERTER = new BasisOfRecordConverter();

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
    final Double latitude = occurrence.getLatitude();
    final Double longitude = occurrence.getLongitude();
    final Integer geospatialIssue = occurrence.getGeospatialIssue();

    doc.setField(KEY.getFieldName(), occurrence.getKey());
    doc.setField(YEAR.getFieldName(), occurrence.getOccurrenceYear());
    doc.setField(MONTH.getFieldName(), occurrence.getOccurrenceMonth());
    doc.setField(BASIS_OF_RECORD.getFieldName(), BOR_CONVERTER.fromEnum(occurrence.getBasisOfRecord()));
    doc.setField(CATALOG_NUMBER.getFieldName(), occurrence.getCatalogNumber());
    doc.setField(COLLECTOR_NAME.getFieldName(), occurrence.getCollectorName());
    doc.setField(COUNTRY.getFieldName(), occurrence.getCountry() == null ? null : occurrence.getCountry()
      .getIso2LetterCode());
    doc.setField(PUBLISHING_COUNTRY.getFieldName(), occurrence.getHostCountry() == null ? null : occurrence
      .getHostCountry().getIso2LetterCode());
    doc.setField(DATASET_KEY.getFieldName(), occurrence.getDatasetKey().toString());
    Set<Integer> taxonKey = buildTaxonKey(occurrence);
    if (!taxonKey.isEmpty()) {
      doc.setField(TAXON_KEY.getFieldName(), taxonKey);
    } else {
      doc.setField(TAXON_KEY.getFieldName(), null);
    }
    doc.setField(ALTITUDE.getFieldName(), occurrence.getAltitude());
    doc.setField(DEPTH.getFieldName(), occurrence.getDepth());
    doc.setField(INSTITUTION_CODE.getFieldName(), occurrence.getInstitutionCode());
    doc.setField(COLLECTION_CODE.getFieldName(), occurrence.getCollectionCode());
    doc.setField(GEOSPATIAL_ISSUE.getFieldName(), geospatialIssue != null && geospatialIssue > 0);
    doc.setField(LATITUDE.getFieldName(), latitude);
    doc.setField(LONGITUDE.getFieldName(), longitude);
    doc.setField(GEOREFERENCED.getFieldName(), latitude != null && longitude != null);
    doc.setField(DATE.getFieldName(),
      occurrence.getOccurrenceDate() != null ? toDateQueryFormat(occurrence.getOccurrenceDate()) : null);
    doc.setField(MODIFIED.getFieldName(),
      occurrence.getModified() != null ? toDateQueryFormat(occurrence.getModified()) : null);
    if (isValidCoordinate(latitude, longitude)) {
      doc.setField(COORDINATE.getFieldName(), COORD_JOINER.join(latitude, longitude));
    } else {
      doc.setField(COORDINATE.getFieldName(), null);
    }

    return doc;
  }


  /**
   * Return a set of integer that contains the taxon key values.
   */
  private Set<Integer> buildTaxonKey(Occurrence occurrence) {

    Set<Integer> taxonKey = new HashSet<Integer>();

    if (occurrence.getNubKey() != null) {
      taxonKey.add(occurrence.getNubKey());
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

    if (occurrence.getSpeciesKey() != null) {
      taxonKey.add(occurrence.getSpeciesKey());
    }

    return taxonKey;
  }

}
