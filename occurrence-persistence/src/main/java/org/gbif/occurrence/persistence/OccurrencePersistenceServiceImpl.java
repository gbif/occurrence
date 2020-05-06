package org.gbif.occurrence.persistence;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An implementation of OccurrenceService for retrieving Occurrence objects in HBase.
 */
@Singleton
public class OccurrencePersistenceServiceImpl implements OccurrenceService {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrencePersistenceServiceImpl.class);

  private final String fragmenterTableName;
  private final int fragmenterSalt;
  private final Connection connection;

  @Inject
  public OccurrencePersistenceServiceImpl(OccHBaseConfiguration cfg, Connection connection) {
    this.fragmenterTableName = checkNotNull(cfg.fragmenterTable, "fragmenterTable can't be null");
    this.fragmenterSalt = cfg.fragmenterSalt;
    this.connection = checkNotNull(connection, "connection can't be null");
  }

  /**
   * Note that the returned fragment here is a String that holds the actual xml or json snippet for this occurrence,
   * and not the Fragment object that is used elsewhere.
   *
   * @param key that identifies an occurrence
   * @return a String holding the original xml or json snippet for this occurrence
   */
  @Override
  public String getFragment(long key) {
    String fragment = null;
    try (Table table = connection.getTable(TableName.valueOf(fragmenterTableName))) {

      String saltedKey = getSaltedKey(key);

      Get get = new Get(Bytes.toBytes(saltedKey));
      Result result = table.get(get);
      if (result == null || result.isEmpty()) {
        LOG.info("Couldn't find occurrence for id [{}], returning null", key);
        return null;
      }
      byte[] rawFragment = result.getValue(Bytes.toBytes("fragment"), Bytes.toBytes("record"));
      if (rawFragment != null) {
        fragment = Bytes.toString(rawFragment);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
    }
    return fragment;
  }

  @Nullable
  @Override
  public VerbatimOccurrence getVerbatim(@Nullable Long key) {
    throw new UnsupportedOperationException("Replaced by pipelines");
  }

  @Override
  public Occurrence get(@Nullable Long key) {
    throw new UnsupportedOperationException("Replaced by pipelines");
  }

  private String getSaltedKey(long key) {
    long mod = key % fragmenterSalt;
    String saltedKey = mod + ":" + key;
    return mod >= 10 ? saltedKey : "0" + saltedKey;
  }
}
