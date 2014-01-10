package org.gbif.occurrence.download.file;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.common.converter.BasisOfRecordConverter;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.common.download.HiveFieldUtil.getHiveField;
import static org.gbif.occurrence.persistence.OccurrenceResultReader.getDate;
import static org.gbif.occurrence.persistence.OccurrenceResultReader.getDouble;
import static org.gbif.occurrence.persistence.OccurrenceResultReader.getInteger;
import static org.gbif.occurrence.persistence.OccurrenceResultReader.getString;
import static org.gbif.occurrence.persistence.OccurrenceResultReader.getUuid;

/**
 * Reads a occurrence record from HBase and return it in a Map<String,Object>.
 */
public class OccurrenceMapReader {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceMapReader.class);

  private final String occurrenceTableName;
  private final HTablePool tablePool;

  private static final BasisOfRecordConverter BOR_CONVERTER = new BasisOfRecordConverter();


  @Inject
  public OccurrenceMapReader(@Named("occurrence_table_name") String occurrenceTableName, HTablePool tablePool) {
    this.occurrenceTableName = occurrenceTableName;
    this.tablePool = tablePool;
  }

  /**
   * Creates a Map that contains an occurrence record.
   * The key values are taken from the enumeration FieldName.
   *
   * @return A Map, or an empty map if the result row parameter is null or empty.
   */
  public static Map<String, Object> buildOccurrence(@Nullable Result row) {
    if (row != null && !row.isEmpty()) {
      Map<String, Object> occ = Maps.newHashMap();
      Integer key = Bytes.toInt(row.getRow());
      occ.put(getHiveField(FieldName.ID), key);
      occ.put(getHiveField(FieldName.DATASET_KEY), getUuid(row, FieldName.DATASET_KEY));
      occ.put(getHiveField(FieldName.INSTITUTION_CODE), getCleanString(row, FieldName.INSTITUTION_CODE));
      occ.put(getHiveField(FieldName.COLLECTION_CODE), getCleanString(row, FieldName.COLLECTION_CODE));
      occ.put(getHiveField(FieldName.CATALOG_NUMBER), getCleanString(row, FieldName.CATALOG_NUMBER));
      occ.put(getHiveField(FieldName.I_BASIS_OF_RECORD),
        BOR_CONVERTER.toEnum(getInteger(row, FieldName.I_BASIS_OF_RECORD)));
      occ.put(getHiveField(FieldName.I_SCIENTIFIC_NAME), getString(row, FieldName.I_SCIENTIFIC_NAME));
      occ.put(getHiveField(FieldName.AUTHOR), getCleanString(row, FieldName.AUTHOR));
      occ.put(getHiveField(FieldName.I_NUB_ID), getInteger(row, FieldName.I_NUB_ID));
      occ.put(getHiveField(FieldName.I_KINGDOM), getString(row, FieldName.I_KINGDOM));
      occ.put(getHiveField(FieldName.I_PHYLUM), getString(row, FieldName.I_PHYLUM));
      occ.put(getHiveField(FieldName.I_CLASS), getString(row, FieldName.I_CLASS));
      occ.put(getHiveField(FieldName.I_ORDER), getString(row, FieldName.I_ORDER));
      occ.put(getHiveField(FieldName.I_FAMILY), getString(row, FieldName.I_FAMILY));
      occ.put(getHiveField(FieldName.I_GENUS), getString(row, FieldName.I_GENUS));
      occ.put(getHiveField(FieldName.I_SPECIES), getString(row, FieldName.I_SPECIES));
      occ.put(getHiveField(FieldName.I_KINGDOM_ID), getInteger(row, FieldName.I_KINGDOM_ID));
      occ.put(getHiveField(FieldName.I_PHYLUM_ID), getInteger(row, FieldName.I_PHYLUM_ID));
      occ.put(getHiveField(FieldName.I_CLASS_ID), getInteger(row, FieldName.I_CLASS_ID));
      occ.put(getHiveField(FieldName.I_ORDER_ID), getInteger(row, FieldName.I_ORDER_ID));
      occ.put(getHiveField(FieldName.I_FAMILY_ID), getInteger(row, FieldName.I_FAMILY_ID));
      occ.put(getHiveField(FieldName.I_GENUS_ID), getInteger(row, FieldName.I_GENUS_ID));
      occ.put(getHiveField(FieldName.I_SPECIES_ID), getInteger(row, FieldName.I_SPECIES_ID));
      occ.put(getHiveField(FieldName.I_ISO_COUNTRY_CODE), getString(row, FieldName.I_ISO_COUNTRY_CODE));
      occ.put(getHiveField(FieldName.HOST_COUNTRY), getString(row, FieldName.HOST_COUNTRY));
      occ.put(getHiveField(FieldName.OWNING_ORG_KEY), getUuid(row, FieldName.OWNING_ORG_KEY));
      occ.put(getHiveField(FieldName.I_LATITUDE), getDouble(row, FieldName.I_LATITUDE));
      occ.put(getHiveField(FieldName.I_LONGITUDE), getDouble(row, FieldName.I_LONGITUDE));
      occ.put(getHiveField(FieldName.I_YEAR), getInteger(row, FieldName.I_YEAR));
      occ.put(getHiveField(FieldName.I_MONTH), getInteger(row, FieldName.I_MONTH));
      occ.put(getHiveField(FieldName.I_OCCURRENCE_DATE), toISO8601Date(getDate(row, FieldName.I_OCCURRENCE_DATE)));
      occ.put(getHiveField(FieldName.I_ALTITUDE), getInteger(row, FieldName.I_ALTITUDE));
      occ.put(getHiveField(FieldName.I_DEPTH), getInteger(row, FieldName.I_DEPTH));
      occ.put(getHiveField(FieldName.SCIENTIFIC_NAME), getCleanString(row, FieldName.SCIENTIFIC_NAME));
      occ.put(getHiveField(FieldName.RANK), getCleanString(row, FieldName.RANK));
      occ.put(getHiveField(FieldName.KINGDOM), getCleanString(row, FieldName.KINGDOM));
      occ.put(getHiveField(FieldName.PHYLUM), getCleanString(row, FieldName.PHYLUM));
      occ.put(getHiveField(FieldName.CLASS), getCleanString(row, FieldName.CLASS));
      occ.put(getHiveField(FieldName.ORDER), getCleanString(row, FieldName.ORDER));
      occ.put(getHiveField(FieldName.FAMILY), getCleanString(row, FieldName.FAMILY));
      occ.put(getHiveField(FieldName.GENUS), getCleanString(row, FieldName.GENUS));
      occ.put(getHiveField(FieldName.SPECIES), getCleanString(row, FieldName.SPECIES));
      occ.put(getHiveField(FieldName.SUBSPECIES), getCleanString(row, FieldName.SUBSPECIES));
      occ.put(getHiveField(FieldName.LATITUDE), getCleanString(row, FieldName.LATITUDE));
      occ.put(getHiveField(FieldName.LONGITUDE), getCleanString(row, FieldName.LONGITUDE));
      occ.put(getHiveField(FieldName.LAT_LNG_PRECISION), getCleanString(row, FieldName.LAT_LNG_PRECISION));
      occ.put(getHiveField(FieldName.MAX_ALTITUDE), getCleanString(row, FieldName.MAX_ALTITUDE));
      occ.put(getHiveField(FieldName.MIN_ALTITUDE), getCleanString(row, FieldName.MIN_ALTITUDE));
      occ.put(getHiveField(FieldName.ALTITUDE_PRECISION), getCleanString(row, FieldName.ALTITUDE_PRECISION));
      occ.put(getHiveField(FieldName.MIN_DEPTH), getCleanString(row, FieldName.MIN_DEPTH));
      occ.put(getHiveField(FieldName.MAX_DEPTH), getCleanString(row, FieldName.MAX_DEPTH));
      occ.put(getHiveField(FieldName.DEPTH_PRECISION), getCleanString(row, FieldName.DEPTH_PRECISION));
      occ.put(getHiveField(FieldName.CONTINENT_OCEAN), getCleanString(row, FieldName.CONTINENT_OCEAN));
      occ.put(getHiveField(FieldName.STATE_PROVINCE), getCleanString(row, FieldName.STATE_PROVINCE));
      occ.put(getHiveField(FieldName.COUNTY), getCleanString(row, FieldName.COUNTY));
      occ.put(getHiveField(FieldName.COUNTRY), getCleanString(row, FieldName.COUNTRY));
      occ.put(getHiveField(FieldName.COLLECTOR_NAME), getCleanString(row, FieldName.COLLECTOR_NAME));
      occ.put(getHiveField(FieldName.LOCALITY), getCleanString(row, FieldName.LOCALITY));
      occ.put(getHiveField(FieldName.YEAR), getCleanString(row, FieldName.YEAR));
      occ.put(getHiveField(FieldName.MONTH), getCleanString(row, FieldName.MONTH));
      occ.put(getHiveField(FieldName.DAY), getCleanString(row, FieldName.DAY));
      occ.put(getHiveField(FieldName.BASIS_OF_RECORD), getCleanString(row, FieldName.BASIS_OF_RECORD));
      occ.put(getHiveField(FieldName.IDENTIFIER_NAME), getCleanString(row, FieldName.IDENTIFIER_NAME));
      occ.put(getHiveField(FieldName.IDENTIFICATION_DATE), toISO8601Date(getDate(row, FieldName.IDENTIFICATION_DATE)));
      occ.put(getHiveField(FieldName.PROTOCOL), getString(row, FieldName.PROTOCOL));
      occ.put(getHiveField(FieldName.CREATED), toISO8601Date(getDate(row, FieldName.CREATED)));
      occ.put(getHiveField(FieldName.MODIFIED), toISO8601Date(getDate(row, FieldName.MODIFIED)));
      return occ;
    }
    return null;
  }

  /**
   * Cleans specials characters from a string value.
   * Removes tabs, line breaks and new lines.
   */
  public static String getCleanString(Result row, FieldName column) {
    String value = getString(row, column);
    return value != null ? value.replaceAll(DownloadUtils.DELIMETERS_MATCH, " ") : value;
  }

  /**
   * Converts a date object into a String in IS0 8601 format.
   */
  public static String toISO8601Date(Date date) {
    if (date != null) {
      return new SimpleDateFormat(DownloadUtils.ISO_8601_FORMAT).format(date);
    }
    return null;
  }

  /**
   * Reads an occurrence record from HBase into Map.
   * The occurrence record
   */
  public Map<String, Object> get(@Nonnull Integer key) throws IOException {
    Preconditions.checkNotNull(key, "Ocurrence key can't be null");
    Map<String, Object> occurrence = null;
    HTableInterface table = null;
    Closer closer = Closer.create();
    try {
      table = tablePool.getTable(occurrenceTableName);
      closer.register(table);
      Get get = new Get(Bytes.toBytes(key));
      Result result = table.get(get);
      if (result == null || result.isEmpty()) {
        LOG.debug("Couldn't find occurrence for key [{}], returning null", key);
        return null;
      }
      occurrence = buildOccurrence(result);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
    } finally {
      closer.close();
    }
    return occurrence;
  }
}
