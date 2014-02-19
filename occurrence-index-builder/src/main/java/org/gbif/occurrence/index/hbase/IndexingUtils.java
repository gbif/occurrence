package org.gbif.occurrence.index.hbase;

import org.gbif.occurrence.persistence.hbase.ExtResultReader;
import org.gbif.occurrence.persistence.hbase.FieldName;
import org.gbif.occurrence.index.solr.OccurrenceIndexDocument;
import org.gbif.occurrence.persistence.hbase.FieldNameUtil;

import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.google.common.collect.Range;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.common.search.util.QueryUtils.toDateQueryFormat;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.BASIS_OF_RECORD;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.CATALOG_NUMBER;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COLLECTION_CODE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COORDINATE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COUNTRY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.DATASET_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.DEPTH;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.ELEVATION;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.EVENT_DATE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.HAS_COORDINATE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.INSTITUTION_CODE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LAST_INTERPRETED;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LATITUDE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LONGITUDE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.MONTH;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.PUBLISHING_COUNTRY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.RECORDED_BY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.TAXON_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.YEAR;


/**
 * Utility class for reading Hbase and writing Solr occurrence records.
 */
public class IndexingUtils {

  private static final Long TIMER_THRESHOLD = 600L;

  private static final Logger LOG = LoggerFactory.getLogger(IndexingUtils.class);

  private static final Joiner COORD_JOINER = Joiner.on(',').useForNull("");

  private static final Range<Double> LAT_RANGE = Range.closed(-90.0, 90.0);
  private static final Range<Double> LNG_RANGE = Range.closed(-180.0, 180.0);

  /**
   * Default private constructor.
   */
  private IndexingUtils() {
    // empty block
  }

  /**
   * Adds the solr document to instance managed by the solr server.
   * Exceptions {@link SolrServerException} and {@link IOException} are swallowed.
   */
  public static void addDocumentQuietly(SolrServer solrServer, SolrInputDocument doc) {
    try {
      Long startTime = System.nanoTime();
      solrServer.add(doc);
      Long endTime = System.nanoTime();
      Long totalTime = TimeUnit.NANOSECONDS.toSeconds(endTime - startTime);
      if (TIMER_THRESHOLD <= totalTime) {
        LOG
          .info("Solr is delaying more than {} adding a document, the time adding a document was: {} ",
            TIMER_THRESHOLD,
            totalTime);
      }
    } catch (Exception e) {
      LOG.error("Error adding document", e);
    }
  }

  /**
   * Populates the Solr document using the result row parameter.
   */
  public static void buildOccSolrDocument(Result row, SolrInputDocument doc) {
    final Double latitude = ExtResultReader.getDouble(row, FieldName.I_DECIMAL_LATITUDE);
    final Double longitude = ExtResultReader.getDouble(row, FieldName.I_DECIMAL_LONGITUDE);
//    final Integer geospatialIssue = OccurrenceResultReader.getInteger(row, FieldName.I_GEOSPATIAL_ISSUE);
    final Date occurrenceDate = ExtResultReader.getDate(row, FieldName.I_EVENT_DATE);
    final Date modified = ExtResultReader.getDate(row, FieldName.I_MODIFIED);

    doc.setField(KEY.getFieldName(), ExtResultReader.getKey(row));
    doc.setField(YEAR.getFieldName(), ExtResultReader.getInteger(row, FieldName.I_YEAR));
    doc.setField(MONTH.getFieldName(), ExtResultReader.getInteger(row, FieldName.I_MONTH));
    doc.setField(BASIS_OF_RECORD.getFieldName(), ExtResultReader.getInteger(row, FieldName.I_BASIS_OF_RECORD));
    doc.setField(CATALOG_NUMBER.getFieldName(), ExtResultReader.getString(row, FieldName.CATALOG_NUMBER));
//    doc.setField(RECORDED_BY.getFieldName(), OccurrenceResultReader.getString(row, FieldName.COLLECTOR_NAME));
    doc.setField(COUNTRY.getFieldName(), ExtResultReader.getString(row, FieldName.I_COUNTRY));
    doc
      .setField(PUBLISHING_COUNTRY.getFieldName(), ExtResultReader.getString(row, FieldName.PUB_COUNTRY_CODE));
    doc.setField(DATASET_KEY.getFieldName(), ExtResultReader.getString(row, FieldName.DATASET_KEY));

    Set<Integer> taxonKey = buildTaxonKey(row);
    if (!taxonKey.isEmpty()) {
      doc.setField(TAXON_KEY.getFieldName(), taxonKey);
    } else {
      doc.setField(TAXON_KEY.getFieldName(), null);
    }
    doc.setField(ELEVATION.getFieldName(), ExtResultReader.getInteger(row, FieldName.I_ELEVATION));
    doc.setField(DEPTH.getFieldName(), ExtResultReader.getInteger(row, FieldName.I_DEPTH));
    doc.setField(INSTITUTION_CODE.getFieldName(), ExtResultReader.getString(row, FieldName.INSTITUTION_CODE));
    doc.setField(COLLECTION_CODE.getFieldName(), ExtResultReader.getString(row, FieldName.COLLECTION_CODE));
//    doc.setField(GEOSPATIAL_ISSUE.getFieldName(), geospatialIssue != null && geospatialIssue > 0);
    doc.setField(HAS_COORDINATE.getFieldName(), latitude != null && longitude != null);
    doc.setField(LATITUDE.getFieldName(), latitude);
    doc.setField(LONGITUDE.getFieldName(), longitude);
    doc.setField(EVENT_DATE.getFieldName(), occurrenceDate != null ? toDateQueryFormat(occurrenceDate) : null);
    doc.setField(LAST_INTERPRETED.getFieldName(), modified != null ? toDateQueryFormat(modified) : null);
    if (isValidCoordinate(latitude, longitude)) {
      doc.setField(COORDINATE.getFieldName(), COORD_JOINER.join(latitude, longitude));
    } else {
      doc.setField(COORDINATE.getFieldName(), null);
    }
  }

  /**
   * Populates the Solr document using the result row parameter.
   */
  public static OccurrenceIndexDocument buildOccurrenceObject(Result row) {
    OccurrenceIndexDocument occurrenceIndexDocument = new OccurrenceIndexDocument();
    occurrenceIndexDocument.setKey(ExtResultReader.getKey(row));
    occurrenceIndexDocument.setLatitude(ExtResultReader.getDouble(row, FieldName.I_DECIMAL_LATITUDE));
    occurrenceIndexDocument.setLongitude(ExtResultReader.getDouble(row, FieldName.I_DECIMAL_LONGITUDE));
    occurrenceIndexDocument.setIsoCountryCode(ExtResultReader.getString(row, FieldName.I_COUNTRY));
    occurrenceIndexDocument.setYear(ExtResultReader.getInteger(row, FieldName.I_YEAR));
    occurrenceIndexDocument.setMonth(ExtResultReader.getInteger(row, FieldName.I_MONTH));
    occurrenceIndexDocument.setDay(ExtResultReader.getString(row, FieldName.I_DAY));
    occurrenceIndexDocument.setCatalogNumber(ExtResultReader.getString(row, FieldName.CATALOG_NUMBER));
//    occurrenceIndexDocument.setCollectorName(OccurrenceResultReader.getString(row, FieldName.COLLECTOR_NAME));
    occurrenceIndexDocument.setNubKey(ExtResultReader.getInteger(row, FieldName.I_TAXON_KEY));
    occurrenceIndexDocument.setDatasetKey(ExtResultReader.getString(row, FieldName.DATASET_KEY));
    occurrenceIndexDocument.setKingdomKey(ExtResultReader.getInteger(row, FieldName.I_KINGDOM_KEY));
    occurrenceIndexDocument.setPhylumKey(ExtResultReader.getInteger(row, FieldName.I_PHYLUM_KEY));
    occurrenceIndexDocument.setClassKey(ExtResultReader.getInteger(row, FieldName.I_CLASS_KEY));
    occurrenceIndexDocument.setOrderKey(ExtResultReader.getInteger(row, FieldName.I_ORDER_KEY));
    occurrenceIndexDocument.setFamilyKey(ExtResultReader.getInteger(row, FieldName.I_FAMILY_KEY));
    occurrenceIndexDocument.setGenusKey(ExtResultReader.getInteger(row, FieldName.I_GENUS_KEY));
    occurrenceIndexDocument.setSpeciesKey(ExtResultReader.getInteger(row, FieldName.I_SPECIES_KEY));
    occurrenceIndexDocument.setBasisOfRecord(ExtResultReader.getInteger(row, FieldName.I_BASIS_OF_RECORD));
    occurrenceIndexDocument.setLatitude(ExtResultReader.getDouble(row, FieldName.I_DECIMAL_LATITUDE));
    occurrenceIndexDocument.setLongitude(ExtResultReader.getDouble(row, FieldName.I_DECIMAL_LONGITUDE));
    return occurrenceIndexDocument;
  }

  /**
   * Builds the occurrence scan using the that will be indexed in Solr.
   */
  public static Scan buildOccurrenceScan() {
    return buildScan(FieldName.I_DECIMAL_LATITUDE, FieldName.I_DECIMAL_LONGITUDE, FieldName.I_YEAR,
      FieldName.I_MONTH, FieldName.CATALOG_NUMBER, FieldName.I_TAXON_KEY, FieldName.DATASET_KEY, FieldName.I_KINGDOM_KEY,
      FieldName.I_PHYLUM_KEY, FieldName.I_CLASS_KEY, FieldName.I_ORDER_KEY, FieldName.I_FAMILY_KEY, FieldName.I_GENUS_KEY,
      FieldName.I_SPECIES_KEY, FieldName.I_COUNTRY, FieldName.I_DAY, FieldName.I_BASIS_OF_RECORD,
      FieldName.I_ELEVATION, FieldName.I_DEPTH, FieldName.INSTITUTION_CODE,
      FieldName.COLLECTION_CODE, FieldName.I_EVENT_DATE, FieldName.I_MODIFIED);
  }

  /**
   * Populates the Solr document using the result row parameter.
   */
  public static OccurrenceWritable buildOccurrenceWritableObject(Result row) {
    OccurrenceWritable occurrenceWritable = new OccurrenceWritable();
    occurrenceWritable.setKey(ExtResultReader.getKey(row));
    occurrenceWritable.setLatitude(ExtResultReader.getDouble(row, FieldName.I_DECIMAL_LATITUDE));
    occurrenceWritable.setLongitude(ExtResultReader.getDouble(row, FieldName.I_DECIMAL_LONGITUDE));
    occurrenceWritable.setIsoCountryCode(ExtResultReader.getString(row, FieldName.I_COUNTRY));
    occurrenceWritable.setYear(ExtResultReader.getInteger(row, FieldName.I_YEAR));
    occurrenceWritable.setMonth(ExtResultReader.getInteger(row, FieldName.I_MONTH));
    occurrenceWritable.setDay(ExtResultReader.getString(row, FieldName.I_DAY));
    occurrenceWritable.setCatalogNumber(ExtResultReader.getString(row, FieldName.CATALOG_NUMBER));
//    occurrenceWritable.setCollectorName(OccurrenceResultReader.getString(row, FieldName.COLLECTOR_NAME));
    occurrenceWritable.setNubKey(ExtResultReader.getInteger(row, FieldName.I_TAXON_KEY));
    occurrenceWritable.setDatasetKey(ExtResultReader.getString(row, FieldName.DATASET_KEY));
    occurrenceWritable.setKingdomKey(ExtResultReader.getInteger(row, FieldName.I_KINGDOM_KEY));
    occurrenceWritable.setPhylumKey(ExtResultReader.getInteger(row, FieldName.I_PHYLUM_KEY));
    occurrenceWritable.setClassKey(ExtResultReader.getInteger(row, FieldName.I_CLASS_KEY));
    occurrenceWritable.setOrderKey(ExtResultReader.getInteger(row, FieldName.I_ORDER_KEY));
    occurrenceWritable.setFamilyKey(ExtResultReader.getInteger(row, FieldName.I_FAMILY_KEY));
    occurrenceWritable.setGenusKey(ExtResultReader.getInteger(row, FieldName.I_GENUS_KEY));
    occurrenceWritable.setSpeciesKey(ExtResultReader.getInteger(row, FieldName.I_SPECIES_KEY));
    occurrenceWritable.setBasisOfRecord(ExtResultReader.getInteger(row, FieldName.I_BASIS_OF_RECORD));
    return occurrenceWritable;
  }

  /**
   * Creates a scan instance using the list of fields.
   *
   * @param fieldNames list of {@link FieldName}
   * @return a Scan containing the field names
   */
  public static Scan buildScan(FieldName... fieldNames) {
    Scan scan = new Scan();
    for (FieldName fieldName : fieldNames) {
      scan.addColumn(Bytes.toBytes(FieldNameUtil.getColumn(fieldName).getFamilyName()),
        Bytes.toBytes(FieldNameUtil.getColumn(fieldName).getColumnName()));
    }
    return scan;
  }

  /**
   * Converts a {@link OccurrenceIndexDocument} into {@link SolrInputDocument}.
   */
  public static SolrInputDocument toSolrInputDocument(OccurrenceIndexDocument occurrenceIndexDocument) {
    SolrInputDocument doc = new SolrInputDocument();
    final Double latitude = occurrenceIndexDocument.getLatitude();
    final Double longitude = occurrenceIndexDocument.getLongitude();

    doc.setField(KEY.getFieldName(), occurrenceIndexDocument.getKey());
    doc.setField(YEAR.getFieldName(), occurrenceIndexDocument.getYear());
    doc.setField(MONTH.getFieldName(), occurrenceIndexDocument.getMonth());
    doc.setField(CATALOG_NUMBER.getFieldName(), occurrenceIndexDocument.getCatalogNumber());
    doc.setField(RECORDED_BY.getFieldName(), occurrenceIndexDocument.getCollectorName());
    doc.setField(TAXON_KEY.getFieldName(), occurrenceIndexDocument.getNubKey());
    doc.setField(DATASET_KEY.getFieldName(), occurrenceIndexDocument.getDatasetKey());
    doc.setField(TAXON_KEY.getFieldName(), occurrenceIndexDocument.getKingdomKey());
    doc.setField(TAXON_KEY.getFieldName(), occurrenceIndexDocument.getPhylumKey());
    doc.setField(TAXON_KEY.getFieldName(), occurrenceIndexDocument.getClassKey());
    doc.setField(TAXON_KEY.getFieldName(), occurrenceIndexDocument.getOrderKey());
    doc.setField(TAXON_KEY.getFieldName(), occurrenceIndexDocument.getFamilyKey());
    doc.setField(TAXON_KEY.getFieldName(), occurrenceIndexDocument.getGenusKey());
    doc.setField(TAXON_KEY.getFieldName(), occurrenceIndexDocument.getSpeciesKey());
    doc.setField(COUNTRY.getFieldName(), occurrenceIndexDocument.getIsoCountryCode());
    doc.setField(PUBLISHING_COUNTRY.getFieldName(), occurrenceIndexDocument.getHostCountry());
    doc.setField(BASIS_OF_RECORD.getFieldName(), occurrenceIndexDocument.getBasisOfRecord());
    if (latitude != null || longitude != null) {
      doc.setField(COORDINATE.getFieldName(), COORD_JOINER.join(latitude, longitude));
    }
    return doc;
  }

  /**
   * Reads the taxon key values to create a Set of integers with those values.
   */
  private static Set<Integer> buildTaxonKey(Result row) {
    Set<Integer> taxonKey = new HashSet<Integer>();

    Integer taxaKey = ExtResultReader.getInteger(row, FieldName.I_TAXON_KEY);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = ExtResultReader.getInteger(row, FieldName.I_KINGDOM_KEY);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = ExtResultReader.getInteger(row, FieldName.I_PHYLUM_KEY);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = ExtResultReader.getInteger(row, FieldName.I_CLASS_KEY);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = ExtResultReader.getInteger(row, FieldName.I_ORDER_KEY);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = ExtResultReader.getInteger(row, FieldName.I_FAMILY_KEY);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = ExtResultReader.getInteger(row, FieldName.I_GENUS_KEY);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = ExtResultReader.getInteger(row, FieldName.I_SPECIES_KEY);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    return taxonKey;
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
}
