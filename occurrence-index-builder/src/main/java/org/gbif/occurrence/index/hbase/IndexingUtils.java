package org.gbif.occurrence.index.hbase;

import org.gbif.occurrence.index.solr.OccurrenceIndexDocument;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.persistence.OccurrenceResultReader;
import org.gbif.occurrence.persistence.hbase.HBaseFieldUtil;

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
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.ALTITUDE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.BASIS_OF_RECORD;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.CATALOG_NUMBER;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COLLECTION_CODE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COLLECTOR_NAME;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COORDINATE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.COUNTRY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.DATASET_KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.DATE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.DEPTH;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.GEOREFERENCED;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.GEOSPATIAL_ISSUE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.INSTITUTION_CODE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.KEY;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LATITUDE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.LONGITUDE;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.MODIFIED;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.MONTH;
import static org.gbif.occurrence.search.solr.OccurrenceSolrField.PUBLISHING_COUNTRY;
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
    final Double latitude = OccurrenceResultReader.getDouble(row, FieldName.I_LATITUDE);
    final Double longitude = OccurrenceResultReader.getDouble(row, FieldName.I_LONGITUDE);
    final Integer geospatialIssue = OccurrenceResultReader.getInteger(row, FieldName.I_GEOSPATIAL_ISSUE);
    final Date occurrenceDate = OccurrenceResultReader.getDate(row, FieldName.I_OCCURRENCE_DATE);
    final Date modified = OccurrenceResultReader.getDate(row, FieldName.I_MODIFIED);

    doc.setField(KEY.getFieldName(), OccurrenceResultReader.getKey(row));
    doc.setField(YEAR.getFieldName(), OccurrenceResultReader.getInteger(row, FieldName.I_YEAR));
    doc.setField(MONTH.getFieldName(), OccurrenceResultReader.getInteger(row, FieldName.I_MONTH));
    doc.setField(BASIS_OF_RECORD.getFieldName(), OccurrenceResultReader.getInteger(row, FieldName.I_BASIS_OF_RECORD));
    doc.setField(CATALOG_NUMBER.getFieldName(), OccurrenceResultReader.getString(row, FieldName.CATALOG_NUMBER));
    doc.setField(COLLECTOR_NAME.getFieldName(), OccurrenceResultReader.getString(row, FieldName.COLLECTOR_NAME));
    doc.setField(COUNTRY.getFieldName(), OccurrenceResultReader.getString(row, FieldName.I_ISO_COUNTRY_CODE));
    doc.setField(PUBLISHING_COUNTRY.getFieldName(), OccurrenceResultReader.getString(row, FieldName.HOST_COUNTRY));
    doc.setField(DATASET_KEY.getFieldName(), OccurrenceResultReader.getString(row, FieldName.DATASET_KEY));

    Set<Integer> taxonKey = buildTaxonKey(row);
    if (!taxonKey.isEmpty()) {
      doc.setField(TAXON_KEY.getFieldName(), taxonKey);
    } else {
      doc.setField(TAXON_KEY.getFieldName(), null);
    }
    doc.setField(ALTITUDE.getFieldName(), OccurrenceResultReader.getInteger(row, FieldName.I_ALTITUDE));
    doc.setField(DEPTH.getFieldName(), OccurrenceResultReader.getInteger(row, FieldName.I_DEPTH));
    doc.setField(INSTITUTION_CODE.getFieldName(), OccurrenceResultReader.getString(row, FieldName.INSTITUTION_CODE));
    doc.setField(COLLECTION_CODE.getFieldName(), OccurrenceResultReader.getString(row, FieldName.COLLECTION_CODE));
    doc.setField(GEOSPATIAL_ISSUE.getFieldName(), geospatialIssue != null && geospatialIssue > 0);
    doc.setField(GEOREFERENCED.getFieldName(), latitude != null && longitude != null);
    doc.setField(LATITUDE.getFieldName(), latitude);
    doc.setField(LONGITUDE.getFieldName(), longitude);
    doc.setField(DATE.getFieldName(), occurrenceDate != null ? toDateQueryFormat(occurrenceDate) : null);
    doc.setField(MODIFIED.getFieldName(), modified != null ? toDateQueryFormat(modified) : null);
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
    occurrenceIndexDocument.setKey(OccurrenceResultReader.getKey(row));
    occurrenceIndexDocument.setLatitude(OccurrenceResultReader.getDouble(row, FieldName.I_LATITUDE));
    occurrenceIndexDocument.setLongitude(OccurrenceResultReader.getDouble(row, FieldName.I_LONGITUDE));
    occurrenceIndexDocument.setIsoCountryCode(OccurrenceResultReader.getString(row, FieldName.I_ISO_COUNTRY_CODE));
    occurrenceIndexDocument.setYear(OccurrenceResultReader.getInteger(row, FieldName.I_YEAR));
    occurrenceIndexDocument.setMonth(OccurrenceResultReader.getInteger(row, FieldName.I_MONTH));
    occurrenceIndexDocument.setDay(OccurrenceResultReader.getString(row, FieldName.DAY));
    occurrenceIndexDocument.setCatalogNumber(OccurrenceResultReader.getString(row, FieldName.CATALOG_NUMBER));
    occurrenceIndexDocument.setCollectorName(OccurrenceResultReader.getString(row, FieldName.COLLECTOR_NAME));
    occurrenceIndexDocument.setNubKey(OccurrenceResultReader.getInteger(row, FieldName.I_NUB_ID));
    occurrenceIndexDocument.setDatasetKey(OccurrenceResultReader.getString(row, FieldName.DATASET_KEY));
    occurrenceIndexDocument.setKingdomKey(OccurrenceResultReader.getInteger(row, FieldName.I_KINGDOM_ID));
    occurrenceIndexDocument.setPhylumKey(OccurrenceResultReader.getInteger(row, FieldName.I_PHYLUM_ID));
    occurrenceIndexDocument.setClassKey(OccurrenceResultReader.getInteger(row, FieldName.I_CLASS_ID));
    occurrenceIndexDocument.setOrderKey(OccurrenceResultReader.getInteger(row, FieldName.I_ORDER_ID));
    occurrenceIndexDocument.setFamilyKey(OccurrenceResultReader.getInteger(row, FieldName.I_FAMILY_ID));
    occurrenceIndexDocument.setGenusKey(OccurrenceResultReader.getInteger(row, FieldName.I_GENUS_ID));
    occurrenceIndexDocument.setSpeciesKey(OccurrenceResultReader.getInteger(row, FieldName.I_SPECIES_ID));
    occurrenceIndexDocument.setBasisOfRecord(OccurrenceResultReader.getInteger(row, FieldName.I_BASIS_OF_RECORD));
    occurrenceIndexDocument.setLatitude(OccurrenceResultReader.getDouble(row, FieldName.LATITUDE));
    occurrenceIndexDocument.setLongitude(OccurrenceResultReader.getDouble(row, FieldName.LONGITUDE));
    return occurrenceIndexDocument;
  }

  /**
   * Builds the occurrence scan using the that will be indexed in Solr.
   */
  public static Scan buildOccurrenceScan() {
    return buildScan(FieldName.I_LATITUDE, FieldName.I_LONGITUDE, FieldName.I_GEOSPATIAL_ISSUE, FieldName.I_YEAR,
      FieldName.I_MONTH, FieldName.CATALOG_NUMBER, FieldName.I_NUB_ID, FieldName.DATASET_KEY, FieldName.I_KINGDOM_ID,
      FieldName.I_PHYLUM_ID, FieldName.I_CLASS_ID, FieldName.I_ORDER_ID, FieldName.I_FAMILY_ID, FieldName.I_GENUS_ID,
      FieldName.I_SPECIES_ID, FieldName.I_ISO_COUNTRY_CODE, FieldName.DAY, FieldName.I_BASIS_OF_RECORD,
      FieldName.COLLECTOR_NAME, FieldName.I_ALTITUDE, FieldName.I_DEPTH, FieldName.INSTITUTION_CODE,
      FieldName.COLLECTION_CODE, FieldName.I_OCCURRENCE_DATE, FieldName.I_MODIFIED);
  }

  /**
   * Populates the Solr document using the result row parameter.
   */
  public static OccurrenceWritable buildOccurrenceWritableObject(Result row) {
    OccurrenceWritable occurrenceWritable = new OccurrenceWritable();
    occurrenceWritable.setKey(OccurrenceResultReader.getKey(row));
    occurrenceWritable.setLatitude(OccurrenceResultReader.getDouble(row, FieldName.I_LATITUDE));
    occurrenceWritable.setLongitude(OccurrenceResultReader.getDouble(row, FieldName.I_LONGITUDE));
    occurrenceWritable.setIsoCountryCode(OccurrenceResultReader.getString(row, FieldName.I_ISO_COUNTRY_CODE));
    occurrenceWritable.setYear(OccurrenceResultReader.getInteger(row, FieldName.I_YEAR));
    occurrenceWritable.setMonth(OccurrenceResultReader.getInteger(row, FieldName.I_MONTH));
    occurrenceWritable.setDay(OccurrenceResultReader.getString(row, FieldName.DAY));
    occurrenceWritable.setCatalogNumber(OccurrenceResultReader.getString(row, FieldName.CATALOG_NUMBER));
    occurrenceWritable.setCollectorName(OccurrenceResultReader.getString(row, FieldName.COLLECTOR_NAME));
    occurrenceWritable.setNubKey(OccurrenceResultReader.getInteger(row, FieldName.I_NUB_ID));
    occurrenceWritable.setDatasetKey(OccurrenceResultReader.getString(row, FieldName.DATASET_KEY));
    occurrenceWritable.setKingdomKey(OccurrenceResultReader.getInteger(row, FieldName.I_KINGDOM_ID));
    occurrenceWritable.setPhylumKey(OccurrenceResultReader.getInteger(row, FieldName.I_PHYLUM_ID));
    occurrenceWritable.setClassKey(OccurrenceResultReader.getInteger(row, FieldName.I_CLASS_ID));
    occurrenceWritable.setOrderKey(OccurrenceResultReader.getInteger(row, FieldName.I_ORDER_ID));
    occurrenceWritable.setFamilyKey(OccurrenceResultReader.getInteger(row, FieldName.I_FAMILY_ID));
    occurrenceWritable.setGenusKey(OccurrenceResultReader.getInteger(row, FieldName.I_GENUS_ID));
    occurrenceWritable.setSpeciesKey(OccurrenceResultReader.getInteger(row, FieldName.I_SPECIES_ID));
    occurrenceWritable.setBasisOfRecord(OccurrenceResultReader.getInteger(row, FieldName.I_BASIS_OF_RECORD));
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
      scan.addColumn(Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(fieldName).getColumnFamilyName()),
        Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(fieldName).getColumnName()));
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
    doc.setField(COLLECTOR_NAME.getFieldName(), occurrenceIndexDocument.getCollectorName());
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

    Integer taxaKey = OccurrenceResultReader.getInteger(row, FieldName.I_NUB_ID);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = OccurrenceResultReader.getInteger(row, FieldName.I_KINGDOM_ID);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = OccurrenceResultReader.getInteger(row, FieldName.I_PHYLUM_ID);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = OccurrenceResultReader.getInteger(row, FieldName.I_CLASS_ID);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = OccurrenceResultReader.getInteger(row, FieldName.I_ORDER_ID);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = OccurrenceResultReader.getInteger(row, FieldName.I_FAMILY_ID);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = OccurrenceResultReader.getInteger(row, FieldName.I_GENUS_ID);
    if (taxaKey != null) {
      taxonKey.add(taxaKey);
    }

    taxaKey = OccurrenceResultReader.getInteger(row, FieldName.I_SPECIES_ID);
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
